import re
from dataclasses import dataclass
from datetime import date
from typing import Literal

from prefect import get_run_logger
from shared.rapid_api import rapid_api_request


@dataclass
class RoomRate:
    """Represents a room rate from a hotel provider.

    Attributes:
        provider (Literal["booking.com", "hotels.com"]): The provider name.
        hotel_name (str): The name of the hotel.
        room_type (str): The type of the room.
        total (float): The total price for the room.
        policy (str): The cancellation policy or other policy info.
    """

    provider: Literal["booking.com", "hotels.com"]
    hotel_name: str
    room_type: str
    total: float
    policy: str = "Unknown"


def _best_rates(rates: list[RoomRate]) -> list[RoomRate]:
    """Return the best rate for each room and type and policy.

    Args:
        rates (list[RoomRate]): A list of room rates.

    Returns:
        list[RoomRate]: The best room rates per type and policy.
    """
    room_rates = {}

    for rate in rates:
        # take lowest rate per room/policy
        if (rate.room_type, rate.policy) in room_rates:
            if room_rates[(rate.room_type, rate.policy)].total > rate.total:
                room_rates[(rate.room_type, rate.policy)] = rate
        else:
            room_rates[(rate.room_type, rate.policy)] = rate

    rates = list(room_rates.values())

    rates.sort(key=lambda x: x.total)

    return rates


def _process_booking_com_rates(
    hotel_name: str, data: dict, room_filter: set[str], room_patterns: list[str]
) -> list[RoomRate]:
    """Clean and filter booking.com room rates from RapidAPI response.

    Args:
        hotel_name (str): The name of the hotel.
        data (dict): The JSON response from RapidAPI.
        room_filter (set[str]): Optional set of room name patterns to filter by.
        room_patterns (list[str]): Optional list of regexs to replace in room names.

    Returns:
        list[RoomRate]: A list of room rates.
    """

    logger = get_run_logger()

    rates = []

    for room in data["data"]["block"]:
        room_type = room["name"]

        for pattern in room_patterns:
            room_type = re.sub(pattern, "", room_type)

        # ignore rooms not in filter, if provided
        if room_filter and all(
            room_type.lower().strip().find(room) == -1 for room in room_filter
        ):
            continue

        # remove currency formatting from amount
        amount = int(
            room["product_price_breakdown"]["all_inclusive_amount"]["amount_rounded"]
            .replace("£", "")
            .replace(",", "")
        )

        # get cancellation policy
        policy = room["policy_display_details"]["cancellation"]["title_details"][
            "translation"
        ]

        rates.append(
            RoomRate(
                provider="booking.com",
                hotel_name=hotel_name,
                room_type=room_type,
                policy=policy,
                total=amount,
            )
        )

    logger.info(f"{len(rates)} filtered booking.com rates for {hotel_name}")

    return rates


def _process_hotels_com_rates(
    hotel_name: str, data: dict, rooms_filter: set[str], room_patterns: list[str]
) -> list[RoomRate]:
    """Clean and filter hotels.com room rates from RapidAPI response.

    Args:
        hotel_name (str): The name of the hotel.
        data (dict): The JSON response from RapidAPI.
        rooms_filter (set[str]): Optional set of room name patterns to filter by.
        room_patterns (list[str]): Optional list of regexs to replace in room names.

    Returns:
        list[RoomRate]: A list of room rates.
    """

    logger = get_run_logger()

    rates = []

    listings = data["data"]["categorizedListings"] or []

    for listing in listings:
        if len(listing["primarySelections"]) != 1:
            raise ValueError("More than one room in listing.")

        room = listing["primarySelections"][0]["propertyUnit"]
        room_type = room["header"]["text"]

        # ignore rooms not in filter, if provided
        if rooms_filter and all(
            room_type.lower().strip().find(room) == -1 for room in rooms_filter
        ):
            continue

        for pattern in room_patterns:
            room_type = re.sub(pattern, "", room_type)

        for rate in room["ratePlans"]:
            for detail in rate["priceDetails"]:
                amount = detail["lodgingPrepareCheckout"]["action"]["totalPrice"][
                    "amount"
                ]

                # hotels.com does not seem to provide cancellation policy in API response
                rates.append(
                    RoomRate(
                        provider="hotels.com",
                        hotel_name=hotel_name,
                        room_type=room_type,
                        total=amount,
                    )
                )

    logger.info(f"{len(rates)} filtered hotels.com rates for {hotel_name}")
    return rates


def booking_com_rates(
    hotel_name: str,
    hotel_id: int,
    check_in: date,
    check_out: date,
    room_filter: set[str],
    room_patterns: list[str],
    adults: int = 2,
    rooms: int = 1,
) -> list[RoomRate]:
    """Return room rates from booking.com.

    Args:
        hotel_name (str): The name of the hotel.
        hotel_id (int): The booking.com hotel ID.
        check_in (date): The check-in date.
        check_out (date): The check-out date.
        room_filter (set[str]): Optional set of room name patterns to filter by.
        room_patterns (list[str]): Optional list of regexs to replace in room names.
        adults (int, optional): Number of adults. Defaults to 2.
        rooms (int, optional): Number of rooms. Defaults to 1.

    Returns:
        list[RoomRate]: A list of room rates.
    """

    logger = get_run_logger()

    room_filter = {room.lower().strip() for room in room_filter}

    fmt_check_in = check_in.strftime("%Y-%m-%d")
    fmt_check_out = check_out.strftime("%Y-%m-%d")

    query = {
        "hotel_id": str(hotel_id),
        "arrival_date": fmt_check_in,
        "departure_date": fmt_check_out,
        "room_qty": rooms,
        "adults": adults,
        "children_age": "0",
        "languagecode": "en-us",
        "currency_code": "GBP",
    }

    logger.info(
        f"Getting booking.com rates for {hotel_name} from {fmt_check_in} to {fmt_check_out}"
    )

    data = rapid_api_request(
        "https://booking-com15.p.rapidapi.com/api/v1/hotels/getRoomList",
        query=query,
        host="booking-com15.p.rapidapi.com",
    )

    return _best_rates(
        _process_booking_com_rates(hotel_name, data, room_filter, room_patterns)
    )


def hotels_com_rates(
    hotel_name: str,
    hotel_id: str,
    check_in: date,
    check_out: date,
    room_filter: set[str],
    room_patterns: list[str],
) -> list[RoomRate]:
    """Return room rates from hotels.com.

    Args:
        hotel_name (str): The name of the hotel.
        hotel_id (str): The hotels.com region ID and hotel ID concatenated with "_".
        check_in (date): The check-in date.
        check_out (date): The check-out date.
        room_filter (set[str]): Optional set of room name patterns to filter by.
        room_patterns (list[str]): Optional list of regexs to replace in room names.

    Returns:
        list[RoomRate]: A list of room rates.
    """

    logger = get_run_logger()

    room_filter = {room.lower().strip() for room in room_filter}

    fmt_check_in = check_in.strftime("%Y-%m-%d")
    fmt_check_out = check_out.strftime("%Y-%m-%d")

    query = {
        "propertyId": hotel_id,
        "checkinDate": fmt_check_in,
        "checkoutDate": fmt_check_out,
        "regionId": "300066865",  # UK / GBP
    }

    logger.info(
        f"Getting hotels.com rates for {hotel_name} from {fmt_check_in} to {fmt_check_out}"
    )

    data = rapid_api_request(
        "https://hotels-com6.p.rapidapi.com/hotels/details-offers",
        query=query,
        host="hotels-com6.p.rapidapi.com",
    )

    return _best_rates(
        _process_hotels_com_rates(hotel_name, data, room_filter, room_patterns)
    )
