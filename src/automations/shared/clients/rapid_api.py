from typing import Any, Dict, Optional

import httpx

from src.automations.config import RapidApiConfig
from src.automations.shared.exceptions import RapidAPIError


class RapidApiClient:
    def __init__(self, base_url: str, host: str):
        """Initialize the RapidAPI client."""
        self._rapid_config = RapidApiConfig()
        self._api_key = self._rapid_config.api_key
        self._base_url = base_url
        self._host = host

    def _handle_http_error(self, response: httpx.Response) -> None:
        """Handle HTTP errors by raising a RapidApiError with the appropriate message.

        Behavior mirrors the screenshots: attempt to parse a JSON 'message' field
        and fall back to the raw response content if parsing fails.
        """
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                response.read()
                error_data = response.json()
                message = error_data["message"]
            except (ValueError, KeyError):
                message = response.content
            raise RapidAPIError(f"{e.response.status_code} {message}") from e

    def _http_client(self) -> httpx.Client:
        """Create an HTTP client configured for RapidAPI."""
        return httpx.Client(
            base_url=self._base_url,
            event_hooks={"response": [self._handle_http_error]},
            headers={
                "Content-Type": "application/json",
                "X-RapidAPI-Host": self._host,
                "X-RapidAPI-Key": self._api_key.get_secret_value(),
            },
            timeout=60,
        )

    def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        """Make a GET request to the specified endpoint with optional query parameters.

        Args:
            endpoint: The API endpoint to call (relative to the base URL).
            params: Optional query parameters to include in the request.

        Returns:
            httpx.Response: The httpx.Response from the API.
        """
        with self._http_client() as client:
            response = client.get(endpoint, params=params)

        return response

    def post(
        self, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        """Make a POST request to the specified endpoint with optional JSON data.

        Args:
            endpoint: The API endpoint to call (relative to the base URL).
            data: Optional dictionary to send as JSON in the request body.
        Returns:
            httpx.Response: The httpx.Response from the API.
        """
        with self._http_client() as client:
            response = client.post(endpoint, json=data)

        return response
