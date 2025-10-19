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
        font-size: 14px;
        margin: 20px 0;
    }

    .custom-table td, .custom-table th {
        padding: 8px 8px;
        text-align: left;
        border-bottom: 1px solid gray;
    }

    .custom-table th {
        font-weight: 600;
        border-bottom: 2px solid gray;
    }
</style>
"""


class Hotel(BaseModel):
    name: str
    booking_id: int
    hotels_id: str
    room_filter: set[str] = Field(default_factory=set)


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

    html = f"""
<p><b>{stay.name}</b></p>
<table class='custom-table'>
"""

    html += """
<tr>
    <th>Hotel</th>
    <th>Room</th>
    <th>Rate</th>
    <th>Policy</th>
    <th>Provider</th>
</tr>
"""

    for rate in rates:
        html += f"""
<tr>
    <td>{rate.hotel_name}</td>
    <td>{rate.room_type}</td>
    <td>£{rate.total:,.0f}</td>
    <td>{rate.policy}</td>
    <td>{rate.provider}</td>
</tr>
"""

    html += "</table>"

    return html


@flow
def run_report(recipients: tuple[str, ...]):
    html = f"""
<html>
<head>
{CSS}
</head>
"""

    logger = get_run_logger()

    hotels = tuple(Hotel(**h) for h in Variable.get("hotels"))

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
            )
            for hotel in stay.hotels
        ]
        hotels_rates = [
            hotels_com_rates(
                hotel_name=hotel.name,
                hotel_id=hotel.hotels_id,
                check_in=stay.check_in,
                check_out=stay.check_out,
                room_filter=hotel.room_filter,
            )
            for hotel in stay.hotels
        ]

        booking_rates = [rate for sublist in booking_rates for rate in sublist]
        hotels_rates = [rate for sublist in hotels_rates for rate in sublist]

        all_rates = [*booking_rates, *hotels_rates]
        all_rates.sort(key=lambda x: (x.hotel_name, x.total))

        html += stay_html(stay, all_rates)

    html += "</html>"

    send_mail(to=recipients, subject="Daily Hotels Report", body=html)


if __name__ == "__main__":
    recipients = ("axtellpete@gmail.com",)
    run_report(recipients)
