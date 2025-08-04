from Shared.pyspark_env import setEnv
from Shared.sparkconfig import create_spark_session
from SourceUberSatellite.Schema import sourceschema
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from SourceUberSatellite.DataCleaner import DataCleaner
from concurrent.futures import ThreadPoolExecutor, as_completed
from NoteBooks.Logger import Logger
import argparse
import sys
import datetime

def main(output_path,loadtype):
    logging = Logger(notebook_name='Process_UberSatellite')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters: output={output_path},loadtype={loadtype}")
    try:
        setEnv()
        spark = create_spark_session()
        logger.info(" Spark session initialized successfully")

        satellite_tables = ["customerdetails","driverdetails", "vehicledetails"]

        def process_table(sourceobject: str):
            """Load, clean and write one satellite table."""
            schema = sourceschema(sourcedefinition=sourceobject)

            loadio = DataLakeIO(
                process='load',
                table=sourceobject,
                loadtype=loadtype
            )
            dataloader = DataLoader(
                path=loadio.filepath(),
                schema=schema,
                filetype=loadio.file_ext(),
                loadtype=loadtype
            )
            source_data = dataloader.LoadData(spark)

            datacleaner = DataCleaner(
                sourceobject=sourceobject,
                spark=spark,
                loadtype=loadtype
            )
            destination_data = datacleaner.clean(data=source_data)

            currentio = DataLakeIO(
                process="write",
                table=sourceobject,
                state='current',
                loadtype=loadtype,
                layer='raw'
            )
            datawriter = DataWriter(
                loadtype=loadtype,
                path=currentio.filepath(),
                format="delta",
                spark=spark
            )
            datawriter.WriteData(
                df=destination_data
            )

            # Adjust max_workers to the number of tables or your system’s capacity
        with ThreadPoolExecutor(max_workers=len(satellite_tables)) as executor:
            # submit all jobs
            future_to_table = {
                executor.submit(process_table, tbl): tbl
                for tbl in satellite_tables
            }
            # optionally, track progress
            for fut in as_completed(future_to_table):
                tbl  = future_to_table[fut]
                try:
                    result = fut.result()
                    print(f"[{tbl}] completed successfully")
                except Exception as e:
                    print(f"[{tbl}] failed: {e}")

        spark.stop()
        logger.info(f"Processing completed at {datetime.datetime.now()}")
        return 0

    except Exception as e:
        logger.exception(f"Critical error: {str(e)}")
        return 1
if __name__ == "__main__":
    # Force immediate output flushing
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--loadtype", required=True)

    args = parser.parse_args()
    exit_code = main(args.output,args.loadtype)
    sys.exit(exit_code)