import prefect
from prefect import flow


@flow
def test_env():
    import os

    logger = prefect.get_run_logger()
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}")
    logger.info("Importing automations.config...")
    import automations.config

    logger.info(f"automations.config: {automations.config}")
