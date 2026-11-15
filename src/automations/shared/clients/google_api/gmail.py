import base64

from googleapiclient.errors import HttpError
from pydantic import SecretStr

from automations.shared.clients.google_api.base import GoogleClient


class GmailClient(GoogleClient):
    """Client for interacting with the Gmail API."""

    def __init__(
        self,
        client_id: str,
        client_secret: SecretStr,
        refresh_token: SecretStr,
    ) -> None:
        """Initialize the GmailClient."""
        super().__init__(
            service_name="gmail",
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )

    def _get_email_body(self, message: dict) -> str:
        """Extract the email body from the message payload.

        Args:
            message: The email message dictionary.
        Returns:
            The email body as a string.
        """

        mime = message.get("mimeType", "")
        data = message.get("body", {}).get("data")

        if mime == "text/plain" and data:
            decoded = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
            return decoded.decode("utf-8", errors="replace")

        for part in message.get("parts", []):
            result = self._get_email_body(part)
            if result:
                return result

        return ""

    def fetch_email(self, message_id: str, client=None) -> dict:
        """Fetch an email by its message ID.

        Args:
            message_id: The ID of the email message to fetch.
            client: Optional Google API client instance. If not provided, a new client will be built.
        Returns:
            A dictionary containing `id`, `sender`, `subject`, and `body`.
        """

        if not client:
            client = self._build_client()

        with client as service:
            try:
                message = (
                    service.users().messages().get(userId="me", id=message_id).execute()
                )

                headers = {
                    h["name"].lower(): h["value"]
                    for h in message["payload"].get("headers", [])
                }

                return {
                    "id": message["id"],
                    "sender": headers.get("from"),
                    "subject": headers.get("subject"),
                    "body": self._get_email_body(message["payload"]),
                }
            except HttpError as e:
                if e.resp.status == 404:
                    raise ValueError(f"Message ID {message_id} not found.") from e
                raise

    def fetch_received_email_history(
        self,
        history_id: int,
        max_results: int = 100,
        senders_filter: list[str] | None = None,
        exclude_message_ids: list[str] | None = None,
    ) -> list[dict]:
        """Fetch history of received emails from Gmail API.

        Args:
            history_id: The history ID to fetch email history from.
            max_results: The maximum number of results to return.
            senders_filter: Optional list of sender email addresses to filter the results.
            exclude_message_ids: Optional list of message IDs to exclude from the results.
        Returns:
            A list of dictionaries containing the email information.
        """

        added_messages = []

        with self._build_client() as service:
            request = (
                service.users()
                .history()
                .list(
                    userId="me",
                    startHistoryId=history_id,
                    maxResults=max_results,
                    historyTypes=["messageAdded"],
                )
            )
            while request is not None:
                try:
                    response = request.execute()
                except HttpError as e:
                    if e.resp.status == 404:
                        raise ValueError(
                            f"History ID {history_id} not found. It may be too old or invalid."
                        ) from e
                    raise
                added_messages.extend(
                    message
                    for h in response["history"]
                    for message in h.get("messagesAdded", [])
                    if not exclude_message_ids
                    or message["message"]["id"] not in exclude_message_ids
                )

                request = service.users().history().list_next(request, response)

        emails = []

        for message in added_messages:
            message = self.fetch_email(message["message"]["id"])
            if senders_filter is None or any(
                message["sender"].contains(s) for s in senders_filter
            ):
                emails.append(message)

        return emails
