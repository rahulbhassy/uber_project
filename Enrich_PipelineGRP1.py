
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from EnrichUber.NoteBooks import Process_Weather_Uber,Process_Distance_Uber
from EnrichGeoSpatial.NoteBooks import Process_GeospatialTablesRefresh


@task(name="Enrich_Weather_Uber", tags=["enrich", "weather", "uber"])
def enrich_weather_uber_task(uber: str , weather: str, load_type: str, runtype: str = 'prod'):
    """Task to enrich Uber data with weather information"""
    logger = get_run_logger()
    logger.info("Enriching Uber data with weather information")
    Process_Weather_Uber.main(
        uber=uber,
        weather=weather,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Enrich_Distance_Uber", tags=["enrich", "distance", "uber"])
def enrich_distance_uber_task(table: str, loadtype: str, runtype: str):
    logger = get_run_logger()
    """Task to enrich Uber data with distance information"""
    logger.info("Enriching Uber data with distance information")
    Process_Distance_Uber.main(
        table=table,
        loadtype=loadtype,
        runtype=runtype
    )

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

    enrich_weather_uber_task(
        uber="uberfares",
        weather="weatherdetails",
        load_type=load_type,
        runtype=runtype
    )

    enrich_distance_uber_task(
        table="uberfares",
        loadtype=load_type,
        runtype=runtype,
        wait_for=[enrich_weather_uber_task]
    )

    enrich_geospatial_uber_task(
        uber="uberfares",
        borough="features",
        trip="tripdetails",
        loadtype=load_type,
        runtype=runtype,
        wait_for=[enrich_weather_uber_task,enrich_distance_uber_task]
    )

if __name__ == "__main__":
    # Example execution
    enrich_grp1_processing_flow(
        load_type="full",
        runtype="prod"
    )