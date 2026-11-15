from prefect import flow, get_run_logger


@flow(name="process-mail", description="Process incoming mail")
def run(history_id: int):
    logger = get_run_logger()
    logger.info(f"Running process mail with history ID: {history_id}")
