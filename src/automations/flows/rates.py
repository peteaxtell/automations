from dataclasses import dataclass
from datetime import date

from prefect import flow

from automations.shared.hotels import RoomRate, booking_com_rates, hotel_com_rates
from automations.shared.mail import send_mail

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


@dataclass
class Hotel:
    name: str
    booking_id: int
    hotels_id: str


@dataclass
class Stay:
    name: str
    check_in: date
    check_out: date
    hotels: tuple[Hotel, ...]


# Dubai
al_qasr = Hotel("Jumeirah Al Qasr", 73056, "6853839_1498622")
al_naseem = Hotel("Jumeirah Al Naseem", 1988600, "6853839_16850366")
mini_salam = Hotel("Mini Al Salam", 73057, "6853839_973713")

# Tignes
diamond_rock = Hotel("Diamond Rock", 6460454, "184273_51609525")

stays = (
    Stay(
        name="Dubai February 2026",
        check_in=date(2026, 2, 22),
        check_out=date(2026, 2, 28),
        hotels=(al_qasr, al_naseem, mini_salam),
    ),
    Stay(
        name="Tignes March 2026",
        check_in=date(2026, 3, 29),
        check_out=date(2026, 4, 3),
        hotels=(diamond_rock,),
    ),
)


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
    <td>£{rate.total}</td>
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

    for stay in stays:
        booking_rates = [
            booking_com_rates(
                hotel.name, hotel.booking_id, stay.check_in, stay.check_out
            )
            for hotel in stay.hotels
        ]
        hotels_rates = [
            hotel_com_rates(hotel.name, hotel.hotels_id, stay.check_in, stay.check_out)
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
    recipients = ("axtellpete@gmail.com",)
    run_report(recipients)
