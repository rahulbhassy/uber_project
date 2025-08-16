from typing import List
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from PowerBIRefresh.NoteBooks import Process_PowerBIRefresh

@task(name="Save Data To SQL Server", tags=["PowerBI","SQL Server"])
def SaveToSQLServer(configname: List[str],loadtype: str, runtype: str = 'prod'):
    logger = get_run_logger()
    logger.info("Saving tables to sql server")
    Process_PowerBIRefresh.main(
        configname=configname,
        loadtype=loadtype,
        runtype=runtype
    )

@flow(
    name="PowerBIRefresh_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def powerbirefresh_flow(configname: List[str],loadtype: str, runtype: str = 'prod'):
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {loadtype}")

    SaveToSQLServer(
        configname=configname,
        loadtype=loadtype,
        runtype=runtype
    )


if __name__ == "__main__":
    # Example execution
    powerbirefresh_flow(
        configname=['all'],
        loadtype="full",
        runtype="prod"
    )