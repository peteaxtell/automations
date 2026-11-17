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

    prompt = f"""Review {url} and provide a response exactly in the below format.

{{company_name}}
Company Name
{{/company_name}}
{{rns_title}}
Company Name
{{/rns_title}}
{{sentiment}}
How the news will be perceived by investors. One of: {{positive, mildly_positive, neutral, mildly_negative, negative}}.
{{/sentiment}}
{{summary}}
A concise factual summary of the announcement.
{{/summary}}
{{impact}}
The likely share-price impact, with the direction, reasoning and key uncertainties. Do not use or refer to the observed share-price reaction.
{{/impact}}

The summary and impact should be formatted in an email friendly way using HTML.
The summary and impact should be concise, maximum three bullet points per section.
The sentiment should one of the suggested responses only. No other additional text.
Output as raw code.
Do not include any text outside the three sections.
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
def send_rns_summary(subject: str, email_content: str) -> None:
    """Send an email with OpenAI summary of RNS announcement.

    Args:
        subject: The email subject.
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
        # send_mail(to=RECIPIENTS, subject=subject, body=summary)
