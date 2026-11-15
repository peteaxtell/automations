import re

from openai import OpenAI
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

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

    prompt = f"""Review this RNS {url} and produce an emailable HTML summary with the below format. Do not wrap the output in code formatting.
    <html>
    <p>
    <b>{{Company Name - RNS Title}}</b></br>
    {{url}}
    </p>
    <p>{{summary of the announcement}}</p>
    <p>{{likely share price impact summary}}</p>
    </html>
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
def send_rns_summary(email_content: str) -> None:
    """Send an email with OpenAI summary of RNS announcement.

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
        # send_mail(to=RECIPIENTS, subject="New RNS Announcement", body=summary)
