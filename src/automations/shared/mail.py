import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from prefect import task
from prefect.blocks.system import Secret
from prefect.variables import Variable

SMTP_SERVER = Variable.get("mail-server", _sync=True)
SMTP_PORT = 587
EMAIL_USERNAME = Variable.get("mail-username", _sync=True)
EMAIL_PASSWORD = Secret.load("mail-password", _sync=True).get()


@task
def send_mail(to: tuple[str, ...], subject: str, body: str):
    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_USERNAME
    msg["To"] = ";".join(to)
    msg["Subject"] = subject
    html_part = MIMEText(body, "html")
    msg.attach(html_part)

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(EMAIL_USERNAME, EMAIL_PASSWORD)

    text = msg.as_string()
    server.sendmail(EMAIL_USERNAME, to, text)
    server.quit()
