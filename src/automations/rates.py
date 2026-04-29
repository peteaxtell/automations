from dataclasses import dataclass
from datetime import date, datetime

from jinja2 import Environment, FileSystemLoader, select_autoescape
from prefect import flow, get_run_logger, task
from prefect.variables import Variable

from automations.shared.clients.hotels_com import HotelsComClient, HotelsComRate
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
            The parsed date object.
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
        list[Trip]: A list of trips.
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
    ]

    logger.info(f"Loaded {len(trips)} trips: {[t.name for t in trips]}")

    return trips


@task
def get_hotel_rates(
    hotels: Hotel, check_in: date, check_out: date
) -> list[HotelsComRate]:
    """Retrieve hotel rates for the trip from hotels.com API.

    Args:
        hotel: The hotel for which to retrieve rates.
        check_in: The check-in date for the trip.
        check_out: The check-out date for the trip.

    Returns:
        list[HotelsComRate]: A list of hotel rates.
    """

    hotels_com_client = HotelsComClient()

    rates = []

    for hotel in hotels:
        rates.extend(
            hotels_com_client.get_prices(
                city=hotel.city,
                hotel=hotel.name,
                check_in=check_in,
                check_out=check_out,
            )
        )

    return rates


@flow
def run_report(recipients: tuple[str, ...]):
    """Run the daily hotels report and send it by email.

    Args:
        recipients: Tuple of recipient email addresses.
    """

    trips = get_trips()

    for trip in trips:
        trip.rates = get_hotel_rates(
            hotels=trip._requested_hotels,
            check_in=trip.check_in,
            check_out=trip.check_out,
        )

    env = Environment(
        loader=FileSystemLoader("src/automations/templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("email.html.j2")

    html = template.render(trips=trips)

    send_mail(to=recipients, subject="Daily Hotels Report", body=html)


if __name__ == "__main__":
    recipients = ("axtellpete@gmail.com",)
    run_report(recipients)
