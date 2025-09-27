import httpx
from prefect import task
from prefect.blocks.system import Secret

from automations.shared.exceptions import RapidAPIError

RAPID_API_KEY = Secret.load("rapid-api-key", _sync=True).get()


@task
def rapid_api_request(url: str, query: dict[str, str], host: str) -> dict[str, any]:
    """Make a request to the RapidAPI service.

    :param url: The API endpoint URL.
    :param query: The query parameters.
    :param host: The RapidAPI host header.
    :return: The JSON response as a dictionary.
    """

    headers = {"x-rapidapi-key": RAPID_API_KEY, "x-rapidapi-host": host}

    try:
        response = httpx.get(url, headers=headers, params=query, timeout=30)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise RapidAPIError(f"RapidAPI request failed: {e}") from e
