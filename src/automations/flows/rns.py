import json
import re
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from openai import OpenAI
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from automations.shared.mail import send_mail

RECIPIENTS = ("axtellpete@gmail.com",)


@task
def get_summary(url: str) -> str:
    """Get OpenAI summary of RNS announcement from an Investegate URL.

    Args:
        url: The Investegate announcement URL.
    Returns:
        AI summary of the RNS announcement.
    """

    logger = get_run_logger()

    prompt = f"""Review {url} and return one JSON object with exactly these five string properties:
- "company_name": The company name.
- "rns_title": The announcement title.
- "sentiment": How the news will be perceived by investors. One of: positive, mildly_positive, neutral, mildly_negative, negative.
- "summary": A concise factual summary of the announcement, formatted as email-friendly HTML with no more than three bullet points.
- "impact": The likely share-price impact, including direction, reasoning, and key uncertainties. Do not use or refer to the observed share-price reaction. Format as email-friendly HTML with no more than three bullet points.

Return valid JSON only, with no Markdown fences or additional text. Make sure you use that URL exactly, the announcement ID is important."""

    model = Variable.get("openai-model")
    api_key = Secret.load("openai-api-key").get()

    client = OpenAI(api_key=api_key)

    logger.info(
        f"Sending prompt to OpenAI for summary of Investegate announcement {url}"
    )

    response = client.responses.create(
        model=model,
        input=prompt,
        text={"format": {"type": "json_object"}},
    )

    return response.output_text


def parse_response(response: str) -> dict[str, str]:
    """Parse and validate the JSON summary returned by OpenAI."""
    values = json.loads(response)
    if not isinstance(values, dict):
        raise ValueError("OpenAI response must be a JSON object")

    fields = ("company_name", "rns_title", "sentiment", "summary", "impact")
    if set(values) != set(fields):
        raise ValueError("OpenAI response must contain exactly the expected fields")

    parsed: dict[str, str] = {}
    for field in fields:
        value = values[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"OpenAI response field must be a non-empty string: {field}"
            )
        parsed[field] = value.strip()

    return parsed


def send(subject: str, template_values: dict) -> None:
    """Send an email with the market summary.

    Args:
        subject: Email subject
        template_values: A dictionary containing `company_name`, `rns_title`, `sentiment`, `summary` and `impact`.
    """

    environment = Environment(
        loader=FileSystemLoader(Path(__file__).parents[1] / "templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )

    send_mail(
        to=RECIPIENTS,
        subject=subject,
        body=environment.get_template("rns.html").render(rows=template_values),
    )


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

    if len(urls) > 1:
        logger.warning(
            "Found multiple URLs matching RNS pattern. Processing the first only."
        )

    summary = get_summary(urls[0])
    template_values = parse_response(summary)
    send(subject, template_values)


if __name__ == "__main__":
    send_rns_summary(
        "Testing RNS flow",
        "https://www.investegate.co.uk/announcement/9875716",
    )
