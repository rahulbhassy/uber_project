from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from SourceUberFact.DataCleaner import DataCleaner
from Logger import Logger
import argparse
import sys
import datetime

def main(sourceobject, loadtype):
    from SourceUberFact.Schema import sourceschema
    logging = Logger(notebook_name='Process_UberFact')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters:, sourceobject={sourceobject}, loadtype={loadtype}")

    try:
        setVEnv()
        spark = create_spark_session()
        '''
        Fact Tables - uberfares , tripdetails
        '''
        logger.info(" Spark session initialized successfully")
        sourceschema = sourceschema(sourcedefinition=sourceobject)
        # Your actual data processing code here
        loadio = DataLakeIO(
            process='load',
            table=sourceobject,
            loadtype=loadtype
        )
        dataloader = DataLoader(
            path=loadio.filepath(),
            schema=sourceschema,
            filetype=loadio.file_ext(),
            loadtype=loadtype
        )
        source_data = dataloader.LoadData(spark)
        logger.info("Loaded data...")

        datacleaner = DataCleaner(
            sourcedefinition=sourceobject,
            spark=spark,
            loadtype=loadtype
        )
        destination_data = datacleaner.cleandata(sourcedata=source_data)
        logger.info("Data cleaned...")

        currentio = DataLakeIO(
            process="write",
            table=sourceobject,
            state='current',
            layer='raw',
            loadtype=loadtype
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
    parser.add_argument("--sourceobject", required=True)
    parser.add_argument("--loadtype", required=True)

    args = parser.parse_args()
    exit_code = main(args.sourceobject, args.loadtype)
    sys.exit(exit_code)