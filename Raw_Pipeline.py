from distributed.utils import wait_for
from prefect import flow, task
from NoteBooks import Process_UberFact, Process_UberSatellite
from DataGenerator import IncrementalDataGenerator
from prefect_dask.task_runners import DaskTaskRunner  # Optional for parallel runs


# Define task for UberFares processing
@task(name="Load_UberFares", tags=["uber", "etl"], retries=1, retry_delay_seconds=30)
def load_uberfares_task(source_object: str, load_type: str):
    """Task to process Uber fares data"""
    Process_UberFact.main(
        sourceobject=source_object,
        loadtype=load_type
    )


# Define task for data generation
@task(name="Data_Generator", tags=["data-gen"])
def data_generator_task():
    """Task to generate incremental data"""
    IncrementalDataGenerator.main()

@task(name="Load_TripData", tags=["trips","etl"])
def load_tripdata_task(source_object: str, load_type: str):
    """Task to process trip data"""
    Process_UberFact.main(
        sourceobject=source_object,

        loadtype=load_type
    )

# Main workflow
@flow(
    name="Uber_Processing_Pipeline",
    task_runner=DaskTaskRunner(),  # Remove for sequential execution
    description="ETL pipeline for Uber data processing",
    version="1.0"
)
def uber_processing_flow():
    """Orchestrates Uber data processing workflow"""
    # Execute UberFares task with parameters
    load_uberfares_task(
        source_object="uberfares",
        load_type="delta"
    )

    # Execute data generator after UberFares completes
    data_generator_task(wait_for=[load_uberfares_task])

    load_tripdata_task(
        source_object="tripdetails",
        load_type="delta",
        wait_for=[data_generator_task]
    )


# Run the flow
if __name__ == "__main__":
    uber_processing_flow()