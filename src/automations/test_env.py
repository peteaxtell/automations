import prefect
from prefect import flow


@flow
def test_env():
    import os

    logger = prefect.get_run_logger()
    logger.info(f"Current working directory: {os.getcwd()}")
    for key, value in os.environ.items():
        logger.info(f"{key}={value}")
