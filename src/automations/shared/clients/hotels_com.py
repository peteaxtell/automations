"""Client for interacting with the Hotels.com API via RapidAPI."""

from dataclasses import dataclass
from datetime import date
from typing import Dict, List

from prefect import get_run_logger
from pydantic import BaseModel

from src.automations.config import HotelsComConfig
from src.automations.shared.clients.rapid_api import RapidApiClient
from src.automations.shared.exceptions import HotelsComProcessingError


@dataclass
class HotelsComDestination:
    """Destination for Hotels.com API calls."""

    hotel_id: str
    region_id: str


class HotelsComRate(BaseModel):
    """Hotel room rate information."""

    hotel_name: str
    room_name: str
    total: float
    per_night: float

    @property
    def total_formatted(self) -> str:
        """Return the total amount formatted as a currency string."""
        return self._format_currency(self.total)

    @property
    def per_night_formatted(self) -> str:
        """Return the per-night amount formatted as a currency string."""
        return self._format_currency(self.per_night)

    def _format_currency(self, amount: float) -> str:
        """Format a currency amount as a string with the appropriate symbol and formatting."""
        return f"£{amount:,.0f}"


class HotelsComClient(RapidApiClient):
    def __init__(self):
        """Initialize the Hotels.com API client."""
        self._config = HotelsComConfig()
        super().__init__(base_url=self._config.base_url, host=self._config.host)

    def _convert_date_to_dict(self, dt: date) -> Dict[str, int]:
        """Convert a date object to a dictionary with year, month, and day.

        Args:
            dt: The date to convert.
        Returns:
            Dict[str, int]: A dictionary with keys 'year', 'month', and 'day' corresponding to the date
        """
        return {"year": dt.year, "month": dt.month, "day": dt.day}

    def _search(self, query: str) -> List[Dict]:
        """Call the search endpoint.

        Args:
            query: The search query string.
        Returns:
            List[Dict]: A list of search results as dictionaries.
        """
        params = {
            "q": query,
            "locale": self._config.locale,
            "siteId": self._config.site_id,
        }

        response = self.get("locations/v3/search", params=params).json()

        return response.get("sr", [])

    def _get_region_id(self, city: str) -> str:
        """Get the region ID for a given city name.

        Args:
            city: The name of the city to search for.
        Returns:
            str: The region ID corresponding to the city.
        """
        results = self._search(city)

        if not results:
            raise HotelsComProcessingError(f"No regions found for city '{city}'")

        regions = [
            result for result in results if result.get("type") in ("CITY", "MULTICITY")
        ]

        if not regions:
            raise HotelsComProcessingError(f"No regions found for city '{city}'")

        if len(regions) > 1:
            raise HotelsComProcessingError(f"Multiple regions found for city '{city}'")

        return regions[0]["gaiaId"]

    def _get_hotel_id(self, city_name: str, hotel_name: str) -> str | None:
        """Get the hotel ID for a given city and hotel name.

        Args:
            city_name: The name of the city where the hotel is located.
            hotel_name: The name of the hotel to search for.
        Returns:
            str | None: The hotel ID corresponding to the hotel name, or None if not found.
        """
        results = self._search(f"{city_name} {hotel_name}")

        if not results:
            return None

        hotels = [
            result
            for result in results
            if result.get("type") == "HOTEL"
            and hotel_name.lower()
            in result["regionNames"]["primaryDisplayName"].lower()
        ]

        if not hotels:
            return None

        if len(hotels) > 1:
            raise HotelsComProcessingError(
                f"Multiple hotels found for '{hotel_name}' in '{city_name}'"
            )

        return hotels[0]["hotelId"]

    def _get_destination(self, city: str, hotel: str) -> HotelsComDestination | None:
        """Get the destination information for a given city and hotel name.

        Args:
            city: The name of the city where the hotel is located.
            hotel: The name of the hotel to search for.
        Returns:
            HotelsComDestination | None: A HotelsComDestination object containing the hotel and region IDs, or None if the hotel is not found.
        """

        region_id = self._get_region_id(city)
        hotel_id = self._get_hotel_id(city, hotel)

        if not hotel_id:
            return None

        return HotelsComDestination(
            hotel_id=hotel_id,
            region_id=region_id,
        )

    def get_prices(
        self,
        city: str,
        hotel: str,
        check_in: date,
        check_out: date,
        adults: int = 2,
    ) -> List[HotelsComRate]:
        """Get hotel room rates for a given destination and date range.

        Args:
            city: The name of the city where the hotel is located.
            hotel: The name of the hotel to search for.
            check_in: The check-in date.
            check_out: The check-out date.
            adults: The number of adults for the booking (default is 2).
        Returns:
            List[HotelsComRate]: A list of HotelsComRate objects containing room rate information.
        """

        logger = get_run_logger()

        destination = self._get_destination(city, hotel)

        if not destination:
            logger.warning(f"'{hotel}' not found in '{city}'. Skipping.")
            return []

        logger.info(f"Getting prices for '{hotel}' from {check_in} to {check_out}.")

        total_nights = (check_out - check_in).days

        data = {
            "siteId": self._config.site_id,
            "locale": self._config.locale,
            "currency": self._config.currency,
            "propertyId": destination.hotel_id,
            "destination": {"regionId": destination.region_id},
            "checkInDate": self._convert_date_to_dict(check_in),
            "checkOutDate": self._convert_date_to_dict(check_out),
            "rooms": [{"adults": adults}],
        }

        response = self.post("properties/v2/get-offers", data=data).json()

        room_rates: Dict[str, float] = {}

        try:
            listings = response["data"]["propertyOffers"]["categorizedListings"]
        except KeyError:
            raise HotelsComProcessingError("Unexpected response structure")

        for listing in listings:
            primary_selections = listing["primarySelections"]

            if len(primary_selections) > 1:
                raise HotelsComProcessingError(
                    "Multiple elements found under primarySelections"
                )

            unit = primary_selections[0]["propertyUnit"]
            unit_name = unit["header"]["text"]
            rate_plans = unit["ratePlans"]

            for rate_plan in rate_plans:
                for price_detail in rate_plan["priceDetails"]:
                    for option in price_detail["price"]["options"]:
                        price = int(
                            option["formattedDisplayPrice"]
                            .replace("£", "")
                            .replace(",", "")
                        )

                        if unit_name not in room_rates or price < room_rates[unit_name]:
                            room_rates[unit_name] = price

        room_rates_list: List[HotelsComRate] = [
            HotelsComRate(
                hotel_name=hotel,
                room_name=rn,
                total=rt,
                per_night=rt / total_nights,
            )
            for rn, rt in room_rates.items()
        ]

        room_rates_list.sort(key=lambda x: x.total)

        return room_rates_list
