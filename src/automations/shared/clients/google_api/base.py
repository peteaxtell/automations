from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pydantic import SecretStr


class GoogleClient:
    """Base client handling auth for interacting with Google APIs."""

    def __init__(
        self,
        service_name: str,
        client_id: str,
        client_secret: SecretStr,
        refresh_token: SecretStr,
    ):
        """Initialize the GoogleClient."""
        self._service_name = service_name
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token

    @staticmethod
    def _acquire_refresh_token(
        client_id: str, client_secret: SecretStr, scopes: list[str]
    ) -> SecretStr:
        """Acquire a refresh token using the OAuth 2.0 flow.

        Args:
            client_id: The client ID.
            client_secret: The client secret.
            scopes: A list of scopes for which to request access.
        Returns:
            A SecretStr containing the refresh token.
        """

        flow = InstalledAppFlow.from_client_config(
            client_config={
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret.get_secret_value(),
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost"],
                }
            },
            scopes=scopes,
        )

        credentials = flow.run_local_server(
            host="localhost",
            port=0,
            access_type="offline",
            prompt="consent",
            success_message="Sign in successful! You may close this window.",
        )

        if not credentials.refresh_token:
            raise ValueError("No refresh token was obtained.")

        return SecretStr(credentials.refresh_token)

    def _build_client(self):
        """Build the Google API client.

        Returns:
            A Google API client instance for the specified service.
        """
        return build(
            self._service_name,
            "v1",
            credentials=Credentials(
                token=None,
                refresh_token=self._refresh_token.get_secret_value(),
                client_id=self._client_id,
                client_secret=self._client_secret.get_secret_value(),
                token_uri="https://oauth2.googleapis.com/token",
            ),
        )
