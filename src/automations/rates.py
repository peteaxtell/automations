import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl
from openai import OpenAI
from prefect import flow, get_run_logger, task
from prefect.variables import Variable

from automations.config import OpenAIConfig, S3Config
from automations.shared.clients.hotels_com import HotelsComClient, HotelsComRate
from automations.shared.clients.s3_client import S3Client
from automations.shared.exceptions import S3FileNotFoundError
from automations.shared.mail import send_mail


@dataclass
class Hotel:
    name: str
    short_name: str
    city: str


class Trip:
    def __init__(
        self, name: str, check_in: str, check_out: str, hotel_names: list[str]
    ) -> None:
        """Initialize the Trip model.

        Args:
            name: The name of the trip.
            check_in: The check-in date as a string in 'YYYY-MM-DD' format.
            check_out: The check-out date as a string in 'YYYY-MM-DD' format.
            hotel_names: A list of hotel short names associated with the trip.
        """
        self.name = name
        self.check_in = self._parse_date(check_in)
        self.check_out = self._parse_date(check_out)
        self.rates: list[HotelsComRate] = []
        self._resolve_hotels(hotel_names)

    @property
    def check_in_formatted(self) -> str:
        """Return the check-in date formatted as 'DD MMM YYYY'."""
        return self._format_date(self.check_in)

    @property
    def check_out_formatted(self) -> str:
        """Return the check-out date formatted as 'DD MMM YYYY'."""
        return self._format_date(self.check_out)

    def to_csv_format(self) -> list[dict]:
        """Convert the trip data to a dictionary format suitable for CSV output.

        Returns:
           A list of dictionaries, each representing a row of trip data for CSV output.
        """

        return [
            {
                "trip_name": self.name,
                "check_in": self.check_in_formatted,
                "check_out": self.check_out_formatted,
                "hotel_name": rate.hotel_name,
                "room_name": rate.room_name,
                "total": rate.total,
                "per_night": rate.per_night,
            }
            for rate in self.rates
        ]

    @staticmethod
    def _format_date(dt: date) -> str:
        """Format a date object as 'DD MMM'.

        Args:
            dt: The date to format.
        Returns:
            The formatted date string."""
        return dt.strftime("%d %b")

    @staticmethod
    def _parse_date(date_str: str) -> date:
        """Parse date strings in 'YYYY-MM-DD' format into date objects.

        Args:
            date_str: The date string to parse.
        Returns:
            The parsed date.
        """
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    def _resolve_hotels(self, hotel_names: list[str]) -> None:
        """Resolve hotel short names to full hotel information.

        Args:
            hotel_names: A list of hotel short names to resolve.
        """
        logger = get_run_logger()

        hotels = [Hotel(**h) for h in Variable.get("hotels")]

        self._requested_hotels: list[Hotel] = []

        for hotel_name in hotel_names:
            hotel = next((h for h in hotels if h.short_name == hotel_name), None)
            if hotel:
                self._requested_hotels.append(hotel)
            else:
                logger.warning(f"Hotel with short name '{hotel_name}' not found.")


@task
def get_trips() -> list[Trip]:
    """Load and return the list of trips from Prefect variables.

    Returns:
        The configured trips loaded from Prefect variables.
    """

    logger = get_run_logger()

    trips = [
        Trip(
            name=t["name"],
            check_in=t["check_in"],
            check_out=t["check_out"],
            hotel_names=t["hotels"],
        )
        for t in Variable.get("trips")
        if datetime.strptime(t["check_in"], "%Y-%m-%d").date() >= datetime.now().date()
    ]

    logger.info(f"Loaded {len(trips)} trips: {[t.name for t in trips]}")

    return trips


@task
async def get_hotel_rates(
    hotels: list[Hotel], check_in: date, check_out: date
) -> list[HotelsComRate]:
    """Retrieve hotel rates for the trip from hotels.com API.

    Args:
        hotels: The hotels for which to retrieve rates.
        check_in: The check-in date for the trip.
        check_out: The check-out date for the trip.

    Returns:
        Collected hotel rates for the requested hotel(s).
    """

    hotels_com_client = HotelsComClient()

    rates_by_hotel = await asyncio.gather(
        *[
            hotels_com_client.get_prices(
                city=hotel.city,
                hotel=hotel.name,
                check_in=check_in,
                check_out=check_out,
            )
            for hotel in hotels
        ]
    )

    return [rate for hotel_rates in rates_by_hotel for rate in hotel_rates]


@task
def to_csv_format(trips: list[Trip]) -> list[dict]:
    """Convert the list of trips to a format suitable for CSV output.

    Args:
        trips: The list of trips to convert.
    Returns:
        A list of dictionaries representing the trip data in a format suitable for CSV output.
    """

    data = []

    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for trip in trips:
        trip_data = trip.to_csv_format()
        for row in trip_data:
            row["report_date"] = report_date
        data.extend(trip_data)

    return data


@task
def save_to_s3(data: list[dict]) -> None:
    """Append trip data to file in S3.

    Args:
        trips: The list of trips to save as a list of dictionaries.
    """

    logger = get_run_logger()

    filename = Variable.get("report-filename")

    s3_client = S3Client()

    try:
        existing_data = s3_client.download_csv(
            bucket=S3Config().bucket, object_name=filename
        )
    except S3FileNotFoundError:
        logger.info(
            f"No existing data found in S3 for {filename}, new file will be created."
        )
        existing_data = []

    existing_data = [
        row
        for row in existing_data
        if row["report_date"][:10] != datetime.now().strftime("%Y-%m-%d")
    ]

    existing_data.extend(data)

    s3_client.upload_csv(
        bucket=S3Config().bucket, data=existing_data, object_name=filename
    )


@task
def get_summary(data: list[dict]) -> str:
    """Generate an LLM summary of the hotel rates.

    Args:
        data: The list of dictionaries representing the trip data.

    Returns:
        A string summary of the report.
    """

    logger = get_run_logger()

    df = pl.DataFrame(data)

    group_cols = ["trip_name", "check_in", "check_out", "hotel_name", "room_name"]

    lowest = df.sort("total").group_by(group_cols).head(1)
    highest = df.sort("total", descending=True).group_by(group_cols).head(1)
    recent = df.sort("report_date", descending=True).group_by(group_cols).head(2)

    df = pl.concat([lowest, highest, recent]).sort(*group_cols)

    data = df.to_dicts()

    prompt = f"""Given this hotel rates data: {json.dumps(data, indent=2)}
 
Summarise cost changes per hotel per trip_name.
 
The summary should have two sections per trip_name:
1. Availability
- hotel_names for a trip_name that do not have rates for report_date matching current date
2. Rate Changes
- hotel_names for which either:
  a) the total cost has changed by +/-£100 relative to the most recent report_date prior to the current date e.g. yesterday to today has changed +/£100
  b) it is the lowest cost for that trip_name/hotel_name combination and there are no previous rates for that trip_name/hotel_name/room_name i.e. it is the current lowest and the first time a rate exists for this trip_name/hotel_name/room_name
 
for rate changes:
- only include a trip_name/hotel_name/room_name if it has a rate for report_date matching current date
- only include a trip_name/hotel_name/room_name if the total cost has changed by +/-£100
- only use one room_name per trip_name/hotel_name combination for comparison, selecting the room_name per trip_name/hotel_name combination with the lowest total cost
- only compare rates for a given trip_name/hotel_name/room_name with the same trip_name/hotel_name/room_name i.e. do not compare the cost of a Sea View room_name today with a Garden View room_name from yesterday
- if a trip_name/hotel_name/room_name only has rates for report_date matching current_date, and no rates for other report_datee, that qualifies as a +/-£100 move, and should be the room_name used in the summary if it is the cheapest rate for that trip_name/hotel_name
- each included hotel_name should have:
   - cost and name of the cheapest room_name in the format e.g. "Cheapest room ({{room_name}}) is £{{total}} (£{{per_night}} per night)"
   - change from previous report for that trip_name/hotel_name/room_name combination e.g. "Price has increased/reduced by £740 (£74 per night) since the previous report on 17 May"
   - current cost relative to lowest seen for that trip_name/hotel_name/room_name combination "Lowest price for this room was £2,980 on 8 April"
 
do:
    - if no trip_name/hotel_name have availability changes or rate changes, say so as per the second example below, do not add anything else
    - provide the response as html, using the below example styling
    - trip_name name uses h1 styling
    - dates use h2 styling
    - hotel_name name uses h3 styling
    - dates are formatted as Sat 23 May
    - format numbers with a £ sign and commas but no decimals e.g £1,050
do not:
    - use percentages
    - add additional sentences explaining what you've done
    - include a trip_name in the summary if there are no rate changes or availability changes
    - include availability references for a trip_name if there are no availability changes
    - include rate change references for a trip_name if there are no rate changes
    - infer or attempt additional analysis that you think would be helpful
 
example 1:
 
<h1>Ibiza Aug 2026</h1>
<h2>Sat 23 May - Tue 2nd June</h2>
 
<h3>Ibiza Gran Hotel</h3>
<ul>
<li>Cheapest room (Junior Suite Pool View) is £3,740 (£374 per night)
<li>Price has reduced by £740 (£74 per night) since the previous report on 17 May</li>
<li>Lowest total cost for this room was £2,980 on 8 April</li>
</ul>
 
<h3>Destino</h3>
<ul>
<li>Cheapest room (Double Garden View) is £1,400 (£140 per night)
<li>Price has reduced by £100 (£10 per night) since the previous report on 17 May</li>
<li>This is the lowest seen price for this room</li>
</ul>
 
<h1>Cervinia Dec 2026</h1>
<h2>Thu 24 Dec - Mon 28 Dec</h2>
 
<h3>Black Horse Hotel</h3>
<ul>
<li>Cheapest room (Superior Double Mountain View) is £2,800 (£700 per night)
<li>Price has increased £1,000 (£250 per night) since the previous report on 15 May
<li>Lowest total cost for this room was £800 on 8 April</li>
</ul>
 
<h2>White Angel Hotel</h2>
<p>No rates available</p>
 
<p>Full rates table attached</p>
 
example 2:
 
<p>No meaningful change to rates or availability</p>
 
<p>Full rates table attached</p>
 """

    config = OpenAIConfig()

    client = OpenAI(api_key=config.api_key.get_secret_value())

    logger.info("Sending prompt to OpenAI for summary generation.")

    response = client.responses.create(model=config.model, input=prompt)

    return response.output_text


@task
def send_report(content: str, recipients: tuple[str, ...]) -> None:
    """Send the hotel rates report by email.

    Args:
        content: The HTML content to include in the report.
        recipients: Tuple of recipient email addresses.
    """

    send_mail(to=recipients, subject="Daily Hotels Report", body=content)


@flow
def run_report(recipients: tuple[str, ...]) -> None:
    """Get hotel rates, save to S3, and send email report.

    Args:
        recipients: Tuple of recipient email addresses.
    """

    trips = get_trips()

    # submit async tasks so they run concurrently in Prefect
    futures = []
    for trip in trips:
        fut = get_hotel_rates.submit(
            trip._requested_hotels, trip.check_in, trip.check_out
        )
        futures.append((trip, fut))

    # collect results (blocks until each task finishes)
    for trip, fut in futures:
        trip.rates = fut.result()

    csv_data = to_csv_format(trips)

    save_to_s3(csv_data)

    summary = get_summary(csv_data)

    send_report(summary, recipients)


if __name__ == "__main__":
    recipients = ("axtellpete@gmail.com",)
    run_report(recipients)
