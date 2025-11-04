from datetime import date

from jinja2 import Environment, FileSystemLoader, select_autoescape
from prefect import flow, get_run_logger, task
from prefect.variables import Variable
from pydantic import BaseModel, Field
from shared.hotels import booking_com_rates, hotels_com_rates
from shared.mail import send_mail


class Hotel(BaseModel):
    """Represents a hotel with optional booking and hotels.com IDs.

    Attributes:
        name (str): Name of the hotel.
        booking_id (int | None): Booking.com hotel ID.
        hotels_id (str | None): Hotels.com hotel ID.
        enabled (bool): Whether the hotel is enabled.
        room_filter (set[str]): Set of room name patterns to filter by.
        room_patterns (list[str]): List of regex patterns to apply to room names.
    """

    name: str
    booking_id: int | None = None
    hotels_id: str | None = None
    enabled: bool = True
    room_filter: set[str] = Field(default_factory=set)
    room_patterns: list[str] = Field(default_factory=list)


class Stay(BaseModel):
    """Represents a stay with check-in/check-out dates and associated hotels.

    Attributes:
        name (str): Name of the stay.
        check_in (date): Check-in date.
        check_out (date): Check-out date.
        hotels (tuple[Hotel, ...]): Tuple of Hotel objects for the stay.
    """

    name: str
    check_in: date
    check_out: date
    hotels: tuple[Hotel, ...]


@task
def get_hotels() -> tuple[Hotel, ...]:
    """Load and return the list of enabled hotels from Prefect variables.

    Returns:
        tuple[Hotel, ...]: A tuple of enabled Hotel objects.
    """

    logger = get_run_logger()

    hotels = tuple(Hotel(**h) for h in Variable.get("hotels"))
    enabled_hotels = tuple(h for h in hotels if h.enabled)

    logger.info(f"Loaded {len(hotels)} hotels: {[h.name for h in hotels]}")

    return enabled_hotels


@task
def get_stays(hotels: tuple[Hotel, ...]) -> list[Stay]:
    """Load and return the list of stays from Prefect variables.

    Returns:
        list[Stay]: A list of Stay objects.
    """

    logger = get_run_logger()

    stays = []

    for stay in Variable.get("stays"):
        stay_hotels = [h for h in hotels if h.name in stay["hotels"]]
        if stay_hotels:
            stays.append(
                Stay(
                    name=stay["name"],
                    check_in=stay["check_in"],
                    check_out=stay["check_out"],
                    hotels=tuple(stay_hotels),
                )
            )

    logger.info(f"Loaded {len(stays)} stays: {[s.name for s in stays]}")

    return stays


@task
def get_stay_rates(stays: list[Stay]) -> dict[str, list]:
    """Get rates for each stay.

    Args:
        stays (list[Stay]): List of Stay objects.

    Returns:
        dict[str, list]: Dictionary mapping stay names to their rates.
    """
    stay_rates = {}

    for stay in stays:
        booking_rates = [
            booking_com_rates(
                hotel_name=hotel.name,
                hotel_id=hotel.booking_id,
                check_in=stay.check_in,
                check_out=stay.check_out,
                room_filter=hotel.room_filter,
                room_patterns=hotel.room_patterns,
            )
            for hotel in stay.hotels
            if hotel.booking_id
        ]
        hotels_rates = [
            hotels_com_rates(
                hotel_name=hotel.name,
                hotel_id=hotel.hotels_id,
                check_in=stay.check_in,
                check_out=stay.check_out,
                room_filter=hotel.room_filter,
                room_patterns=hotel.room_patterns,
            )
            for hotel in stay.hotels
            if hotel.hotels_id
        ]

        booking_rates = [rate for sublist in booking_rates for rate in sublist]
        hotels_rates = [rate for sublist in hotels_rates for rate in sublist]

        all_rates = [*booking_rates, *hotels_rates]
        all_rates.sort(key=lambda x: (x.hotel_name, x.total))

        stay_rates[stay.name] = all_rates

    return stay_rates


@flow
def run_report(recipients: tuple[str, ...]):
    """Run the daily hotels report and send it by email.

    Args:
        recipients (tuple[str, ...]): Tuple of recipient email addresses.
    """

    hotels = get_hotels()

    stays = get_stays(hotels)

    stay_rates = get_stay_rates(stays)

    env = Environment(
        loader=FileSystemLoader("src/automations/templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("email.html.j2")

    html = template.render(stays=stay_rates)

    send_mail(to=recipients, subject="Daily Hotels Report", body=html)


if __name__ == "__main__":
    recipients = ("axtellpete@gmail.com",)
    run_report(recipients)
