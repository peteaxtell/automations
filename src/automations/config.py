from pydantic_settings import BaseSettings


class HotelsComConfig(BaseSettings):
    """Configuration settings for the Hotels.com API client."""

    base_url: str = "https://hotels4.p.rapidapi.com"
    currency: str = "GBP"
    host: str = "hotels4.p.rapidapi.com"
    locale: str = "en_GB"
    site_id: int = 300000005


class S3Config(BaseSettings):
    """Configuration settings for S3 access."""

    bucket: str = "axtell-automations"
    region: str = "eu-north-1"
    access_key_id: str = "AKIAYSE4OKEFZJ4YGJ4G"
