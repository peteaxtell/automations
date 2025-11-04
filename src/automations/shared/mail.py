import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from prefect import task
from prefect.blocks.system import Secret
from prefect.variables import Variable


@task
def send_mail(to: tuple[str, ...], subject: str, body: str):
    """Send an email with the given subject and HTML body to recipients.

    Args:
        to (tuple[str, ...]): Tuple of recipient email addresses.
        subject (str): The subject of the email.
        body (str): The HTML body of the email.
    """
    smtp_server = Variable.get("mail-server")
    smtp_port = 587
    username = Variable.get("mail-username")
    password = Secret.load("mail-password").get()

    msg = MIMEMultipart("alternative")
    msg["From"] = username
    msg["To"] = ",".join(to)
    msg["Subject"] = subject
    html_part = MIMEText(body, "html")
    msg.attach(html_part)

    server = smtplib.SMTP(host=smtp_server, port=smtp_port, timeout=30)
    server.starttls()
    server.login(username, password)

    text = msg.as_string()
    server.sendmail(username, to, text)
    server.quit()
