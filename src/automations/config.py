from prefect.blocks.system import Secret
from prefect.variables import Variable
from pydantic import SecretStr
from pydantic_settings import BaseSettings


class RapidApiConfig(BaseSettings):
    """Configuration settings for the RapidAPI client."""

    api_key: SecretStr = SecretStr(Secret.load("rapid-api-key").get())


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
    secret_access_key: SecretStr = SecretStr(Secret.load("s3-secret-key").get())


class OpenAIConfig(BaseSettings):
    """Configuration settings for OpenAI API access."""

    api_key: SecretStr = SecretStr(Secret.load("openai-api-key").get())
    model: str = Variable.get("openai-model")
