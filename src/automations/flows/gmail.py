from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from pydantic import SecretStr

from automations.config import S3Config
from automations.flows.rns import send_rns_summary
from automations.shared.clients.google_api.gmail import GmailClient
from automations.shared.clients.s3_client import S3Client
from automations.shared.exceptions import S3FileNotFoundError

MAIL_HANDLERS = {
    "Pete Axtell <axtellpete@gmail.com>": send_rns_summary,
    "Investegate <newsalert@mail.investegate.co.uk>": send_rns_summary,
}


@task
def fetch_processed_config(s3_client: S3Client) -> dict:
    """Retrieve the processed config from S3 or create a new one if it doesn't exist.

    Args:
        s3_client: An instance of S3Client.
    Returns:
        A dictionary containing `last_history_id` and `processed_message_ids`.
    """

    logger = get_run_logger()

    try:
        processed_config = s3_client.download_json(
            bucket=S3Config().bucket, object_name="gmail_processed.json"
        )
    except S3FileNotFoundError:
        logger.info(
            "No existing data found in S3 for gmail_processed.json, new file will be created"
        )
        processed_config = {"last_history_id": 11596414, "processed_message_ids": []}

    return processed_config


def fetch_new_emails(
    last_history_id: int,
    processed_message_ids: list[str],
) -> list[dict]:
    """Fetch new emails from Gmail API based on history ID.

    Args:
        last_history_id: The last processed history ID.
        processed_message_ids: A list of already processed message IDs.
    Returns:
        A list of dictionaries containing the new email information.
    """

    gmail_credentials = Secret.load("gmail-api-credentials").get()

    client = GmailClient(
        client_id=gmail_credentials["client_id"],
        client_secret=SecretStr(gmail_credentials["client_secret"]),
        refresh_token=SecretStr(gmail_credentials["refresh_token"]),
    )
    return client.fetch_received_email_history(
        history_id=last_history_id, exclude_message_ids=processed_message_ids
    )


@flow(name="process-gmail-mailbox-alert")
def process_mailbox_alert(history_id: int) -> None:
    """Retrieve emails added since previous alert was processed and process based on sender."""

    logger = get_run_logger()

    s3_client = S3Client(secret_key=Secret.load("s3-secret-key").get())

    processed_config = fetch_processed_config(s3_client)

    new_emails = fetch_new_emails(
        last_history_id=processed_config["last_history_id"],
        processed_message_ids=processed_config["processed_message_ids"],
    )

    logger.info(f"Fetched {len(new_emails)} unprocessed emails")

    processed_ids = processed_config["processed_message_ids"]

    for email in new_emails:
        handler = MAIL_HANDLERS.get(email["sender"])

        if not handler:
            logger.info(
                f"Skipping email from {email['sender']} as no processing logic is defined for this sender."
            )
            continue

        else:
            logger.info(
                f"Processing email {email['id']} from {email['sender']} with subject: {email['subject']}"
            )
            handler(email["body"])

        processed_ids.append(email["id"])

    processed_config["last_history_id"] = history_id
    processed_config["processed_message_ids"] = processed_ids

    s3_client.upload_json(
        bucket=S3Config().bucket,
        data=processed_config,
        object_name="gmail_processed.json",
    )
