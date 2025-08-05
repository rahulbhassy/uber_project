from prefect import flow, task
from SourceUberFact.NoteBooks import Process_UberFact
from SourceUberSatellite.NoteBooks import Process_UberSatellite
from DataGenerator import IncrementalDataGenerator
from prefect_dask.task_runners import DaskTaskRunner
from SourceWeather.NoteBooks import Process_Weather
from prefect import get_run_logger
# Optional for parallel runs


# Define task for UberFares processing
@task(name="Load_UberFares", tags=["uber", "etl"], retries=1, retry_delay_seconds=30)
def load_uberfares_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process Uber fares data"""
    Process_UberFact.main(
        sourceobject=source_object,
        loadtype=load_type,
        runtype=runtype
    )
# Define task for data generation
@task(name="Data_Generator", tags=["data-gen"])
def data_generator_task(load_type: str,runtype: str):
    """Task to generate incremental data (only for delta loads)"""
    logger = get_run_logger()
    if load_type == "delta":
        logger.info("Running data generator for delta load")
        IncrementalDataGenerator.main(runtype=runtype)
    else:
        logger.info("Skipping data generator for full load")

@task(name="Load_TripData", tags=["trips","etl"])
def load_tripdata_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process trip data"""
    Process_UberFact.main(
        sourceobject=source_object,
        loadtype=load_type,
        runtype=runtype
    )

@task(name="Load_UberSatellite", tags=["uber-satellite", "etl"])
def load_ubersatellite_task(load_type: str,runtype: str = 'prod'):
    """Task to process Uber satellite data"""
    Process_UberSatellite.main(
        loadtype=load_type,
        runtype=runtype
    )

@task(name='Load_Weather', tags=["weather", "etl"])
def load_weather_task(source_object: str, load_type: str,runtype: str = 'prod'):
    """Task to process weather data"""
    Process_Weather.main(
        table=source_object,
        loadtype=load_type,
        runtype=runtype
    )


# Main workflow
@flow(
    name="Raw_Uber_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def raw_processing_flow(load_type: str,runtype: str = 'prod'):
    """Orchestrates Uber data processing workflow"""
    logger = get_run_logger()
    logger.info(f"Starting pipeline with load_type: {load_type}")
    # Execute UberFares task with parameters
    load_uberfares_task(
        source_object="uberfares",
        load_type=load_type,
        runtype=runtype
    )
    load_weather_task(
        source_object="weatherdetails",
        load_type="delta",
        runtype=runtype
    )

    data_generator_task(
        load_type=load_type,
        runtype=runtype,
        wait_for=[load_uberfares_task]
    )

    downstream_dependencies = [
        load_uberfares_task,
        data_generator_task,
        load_weather_task
    ]
    load_tripdata_task(
        source_object="tripdetails",
        load_type=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )

    load_ubersatellite_task(
        load_type=load_type,
        runtype=runtype,
        wait_for=downstream_dependencies
    )

# Run the flow
if __name__ == "__main__":
    raw_processing_flow(load_type='full',runtype='dev')