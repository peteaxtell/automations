import asyncio
import random
from typing import Any, Dict, Optional

import httpx

from automations.config import RapidApiConfig
from automations.shared.exceptions import RapidAPIError


class RapidApiClient:
    def __init__(self, base_url: str, host: str):
        """Initialize the RapidAPI client."""
        self._rapid_config = RapidApiConfig()
        self._api_key = self._rapid_config.api_key
        self._base_url = base_url
        self._host = host

    def _http_client(self) -> httpx.AsyncClient:
        """Create an asynchronous HTTP client configured for RapidAPI."""
        return httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Content-Type": "application/json",
                "X-RapidAPI-Host": self._host,
                "X-RapidAPI-Key": self._api_key.get_secret_value(),
            },
            timeout=60,
        )

    async def _request_with_backoff(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Perform an HTTP request with retries for 429 and transient server errors.

        Uses exponential backoff with jitter. Retries on 429 and 5xx responses.
        """

        max_attempts = 5
        base_delay = 0.5

        attempt = 0
        while True:
            attempt += 1
            async with self._http_client() as client:
                response = await client.request(method, endpoint, **kwargs)

            status = response.status_code

            # If response indicates success, return it
            if 200 <= status < 400:
                return response

            # For retryable statuses (429 or 5xx) perform backoff and retry
            if status in (429,) or (500 <= status < 600):
                if attempt >= max_attempts:
                    # final attempt, return response so caller can inspect/raise
                    return response

                # compute exponential backoff with jitter
                delay = base_delay * (2 ** (attempt - 1))
                jitter = random.uniform(0, delay * 0.1)
                await asyncio.sleep(delay + jitter)
                continue

            # Non-retryable error (4xx other than 429): parse and raise RapidAPIError
            try:
                await response.aread()
            except Exception:
                pass

            try:
                error_data = response.json()
                message = error_data.get("message", response.content)
            except Exception:
                try:
                    message = response.content.decode("utf-8", errors="replace")
                except Exception:
                    message = response.content

            raise RapidAPIError(f"{status} {message}")

    async def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        """Make a GET request to the specified endpoint with optional query parameters.

        Args:
            endpoint: The API endpoint to call (relative to the base URL).
            params: Optional query parameters to include in the request.

        Returns:
            The response object returned by the API.
        """
        response = await self._request_with_backoff("GET", endpoint, params=params)
        return response

    async def post(
        self, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        """Make a POST request to the specified endpoint with optional JSON data.

        Args:
            endpoint: The API endpoint to call (relative to the base URL).
            data: Optional dictionary to send as JSON in the request body.
        Returns:
            The response object returned by the API.
        """
        response = await self._request_with_backoff("POST", endpoint, json=data)
        return response
