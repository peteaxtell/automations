from prefect import flow, get_run_logger


@flow(name="process-mail", description="Process incoming mail")
def run(mail: dict):
    logger = get_run_logger()
    logger.info(f"Running process mail: {mail}")
