from prefect import flow
from prefect.schedules import Schedule

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/peteaxtell/automations.git",
        entrypoint="src/automations/rates.py:run_report",
    ).deploy(
        name="Hotels Report",
        work_pool_name="managed-workpool",
        schedule=Schedule(
            cron="0 10 * * *",  # every day at 10am
        ),
        parameters={"recipients": ["axtellpete@gmail.com", "s.axtell@winton.com"]},
        job_variables={"pip_packages": ["jinja2"]},
    )
