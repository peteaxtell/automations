import httpx
from prefect import task
from prefect.blocks.system import Secret
from shared.exceptions import RapidAPIError

RAPID_API_KEY = Secret.load("rapid-api-key", _sync=True).get()


@task(retries=3, retry_delay_seconds=15)
def rapid_api_request(url: str, query: dict[str, str], host: str) -> dict[str, any]:
    """Make a request to the RapidAPI service.

    Args:
        url (str): The API endpoint URL.
        query (dict[str, str]): The query parameters.
        host (str): The RapidAPI host header.

    Returns:
        dict[str, any]: The JSON response as a dictionary.
    """

    headers = {"x-rapidapi-key": RAPID_API_KEY, "x-rapidapi-host": host}

    try:
        response = httpx.get(url, headers=headers, params=query, timeout=60)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise RapidAPIError(f"RapidAPI request failed: {e}") from e
