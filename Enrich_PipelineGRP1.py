
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from EnrichGeoSpatial.NoteBooks import Process_GeospatialTablesRefresh
from Balancing.NoteBooks import Process_Balancing


@task(name="Enrich_Geospatial_Tables", tags=["enrich", "geospatial"])
def enrich_geospatial_uber_task(uber: str,borough: str,trip : str, loadtype: str, runtype: str = 'prod'):
    """Task to enrich Uber data with geospatial information"""
    logger = get_run_logger()
    logger.info("Enriching Uber data with geospatial information")
    Process_GeospatialTablesRefresh.main(
        uber=uber,
        borough=borough,
        trip=trip,
        loadtype=loadtype,
        runtype=runtype
    )

@task(name="Load_Balancing_EnrichGRP1", tags=["balancing", "etl"])
def load_balancing_enrichgrp1_task(load_type: str,runtype: str = 'prod'):
    """Task to process balancing results"""
    Process_Balancing.main(
        runtype=runtype,
        loadtype=load_type,
        tables=['uber','uberfaresenrich']
    )

@flow(
    name="Enrich_Uber_GRP1_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def enrich_grp1_processing_flow(load_type: str,runtype: str = 'prod'):
    """Orchestrates Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {load_type}")


    enrich_geospatial_uber_task(
        uber="uberfares",
        borough="features",
        trip="tripdetails",
        loadtype=load_type,
        runtype=runtype,
    )

    load_balancing_enrichgrp1_task(
        load_type='full',
        runtype=runtype,
        wait_for=enrich_geospatial_uber_task
    )

if __name__ == "__main__":
    # Example execution
    enrich_grp1_processing_flow(
        load_type="full",
        runtype="dev"
    )
