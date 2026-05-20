from prefect import flow


@flow
def test_env():
    import os

    print(f"Current working directory: {os.getcwd()}")
    for key, value in os.environ.items():
        print(f"{key}={value}")
