from prefect.blocks.system import Secret
from pydantic import SecretStr
from pydantic_settings import BaseSettings


class RapidApiConfig(BaseSettings):
    """Configuration settings for the RapidAPI client."""

    api_key: SecretStr = Secret.load("rapid-api-key").get()


class HotelsComConfig(BaseSettings):
    """Configuration settings for the Hotels.com API client."""

    base_url: str = "https://hotels4.p.rapidapi.com"
    currency: str = "GBP"
    host: str = "hotels4.p.rapidapi.com"
    locale: str = "en_GB"
    site_id: int = 300000005
