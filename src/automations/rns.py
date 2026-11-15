import re

from openai import OpenAI
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from automations.shared.mail import send_mail

RECIPIENTS = ["axtellpete@gmail.com"]


@task
def get_summary(url: str) -> str:
    """Get OpenAI summary of RNS announcement from an Investegate URL.

    Args:
        url: The Investegate announcement URL.
    Returns:
        AI summary of the RNS announcement.
    """

    logger = get_run_logger()

    prompt = f"""Review this RNS {url} and produce an emailable HTML summary with the following format:
    <p>
    {{Company Name - RNS Title}}
    {{url}}
    </p>
    <p>{{summary of the announcement}}</p>
    <p>{{likely share price impact summary}}</p>
 """

    model = Variable.get("openai-model")
    api_key = Secret.load("openai-api-key").get()

    client = OpenAI(api_key=api_key)

    logger.info(
        f"Sending prompt to OpenAI for summary of Investegate announcement {url}"
    )

    response = client.responses.create(model=model, input=prompt)

    return response.output_text


@flow
def process_investegate_email(email_content: str) -> None:
    """Get OpenAI summary of RNS announcement from an Investegate email.

    Args:
        email_content: The email content.
    Returns:
        AI summary of the RNS announcement.
    """

    logger = get_run_logger()

    pattern = r"https?://(?:www\.)?investegate\.co\.uk/announcements?/\d+"

    urls = re.findall(pattern, email_content)

    if not urls:
        logger.info("No Investegate announcement URL found in the email content.")
        return

    for url in urls:
        summary = get_summary(url)
        send_mail(to=RECIPIENTS, subject="New RNS Announcement", body=summary)
