from datetime import date

from prefect import flow, get_run_logger
from prefect.variables import Variable
from pydantic import BaseModel, Field
from shared.hotels import RoomRate, booking_com_rates, hotels_com_rates
from shared.mail import send_mail

CSS = """
    <style>
        .custom-table {
            border-collapse: collapse;
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 12px;
            margin: 20px 0;
            border: 1px solid #666;
        }

        .custom-table td,
        .custom-table th {
            padding: 2px 8px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }

        .custom-table th {
            font-weight: 600;
            background-color: #e8e8e8;
            border-bottom: 1px solid #666;
        }
    </style>
"""


class Hotel(BaseModel):
    name: str
    booking_id: int | None = None
    hotels_id: str | None = None
    enabled: bool = True
    room_filter: set[str] = Field(default_factory=set)
    room_patterns: list[str] = Field(default_factory=list)


class Stay(BaseModel):
    name: str
    check_in: date
    check_out: date
    hotels: tuple[Hotel, ...]


def stay_html(stay: Stay, rates: list[RoomRate]) -> str:
    """Generate an HTML table for the given stay and its room rates.

    :param stay: The Stay object containing stay details.
    :param rates: A list of RoomRate objects for the stay.
    :return: An HTML string representing the table."""

    # Deprecated: now using Jinja template for email HTML
    return ""


@flow
def run_report(recipients: tuple[str, ...]):
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader("src/automations/templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("email.html.j2")
    stays_data = []

    logger = get_run_logger()

    hotels = tuple(Hotel(**h) for h in Variable.get("hotels"))

    hotels = tuple(h for h in hotels if h.enabled)

    logger.info(f"Loaded {len(hotels)} hotels: {[h.name for h in hotels]}")

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

        stays_data.append((stay, all_rates))

    html = template.render(stays=stays_data, css=CSS)

    send_mail(to=recipients, subject="Daily Hotels Report", body=html)


if __name__ == "__main__":
    recipients = ("axtellpete@gmail.com", "s.axtell@winton.com")
    run_report(recipients)
