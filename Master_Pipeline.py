from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from Raw_Pipeline import raw_processing_flow
from Enrich_PipelineGRP1 import enrich_grp1_processing_flow

@flow(
    name="Master_Uber_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def master_processing_flow(load_type: str,runtype: str = 'prod'):
    """Orchestrates the entire Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting master pipeline with load_type: {load_type}")

    # Execute raw processing flow
    logger.info("Starting raw processing flow")
    raw_processing_flow(
        load_type=load_type,
        runtype=runtype
    )

    # Execute enrichment flow for Group 1
    logger.info("Starting enrichment flow for Group 1")
    enrich_grp1_processing_flow(
        load_type=load_type,
        runtype=runtype
    )

if __name__ == "__main__":
    # Example usage
    master_processing_flow(
        load_type="delta",  # or "full" based on your requirement
        runtype='prod'  # or 'dev' based on your environment
    )
