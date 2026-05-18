import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from prefect import task
from prefect.blocks.system import Secret
from prefect.variables import Variable


@task
def send_mail(
    to: tuple[str, ...],
    subject: str,
    body: str,
    attachments: list[tuple[str, bytes]] | None = None,
):
    """Send an email with the given subject and HTML body to recipients.

    Args:
        to: Tuple of recipient email addresses.
        subject: The subject of the email.
        body: The HTML body of the email.
        attachments: Optional list of attachments, where each attachment is a tuple of (filename, content bytes).
    """
    smtp_server = Variable.get("mail-server")
    smtp_port = 587
    username = Variable.get("mail-username")
    password = Secret.load("mail-password").get()

    # Use mixed to allow attachments alongside the HTML alternative part
    msg = MIMEMultipart("mixed")
    msg["From"] = username
    msg["To"] = ",".join(to)
    msg["Subject"] = subject

    # HTML body in an alternative part
    alt = MIMEMultipart("alternative")
    html_part = MIMEText(body, "html")
    alt.attach(html_part)
    msg.attach(alt)

    # Attach any files provided as (filename, bytes)
    if attachments:
        for filename, content in attachments:
            part = MIMEApplication(content)
            part.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(part)

    server = smtplib.SMTP(host=smtp_server, port=smtp_port, timeout=30)
    server.starttls()
    server.login(username, password)

    text = msg.as_string()
    server.sendmail(username, to, text)
    server.quit()
