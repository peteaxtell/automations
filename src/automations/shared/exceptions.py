class RapidAPIError(Exception):
    """Base exception for RapidAPI errors."""


class HotelsComError(Exception):
    """Base exception for Hotels.com API errors."""


class HotelsComProcessingError(HotelsComError):
    """Raised when there is an error processing Hotels.com API data."""


class S3FileNotFoundError(Exception):
    """Raised when a specified file is not found in the S3 bucket."""
