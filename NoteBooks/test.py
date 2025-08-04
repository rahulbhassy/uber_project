from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from SourceUberFact.DataCleaner import DataCleaner
import argparse

def main(output_path, sourceobject, loadtype):
    from SourceUberFact.Schema import sourceschema
    logger = setup_logger()

    # Log critical environment information
    logger.info(f"🚀 Starting Uber Data Processing")
    logger.info(f"👤 Current user: {os.getenv('USERNAME', 'Unknown')}")
    logger.info(f"🏠 User profile: {os.getenv('USERPROFILE', 'Unknown')}")
    logger.info(f"📂 Working directory: {os.getcwd()}")
    logger.info(f"🐍 Python executable: {sys.executable}")
    logger.info(f"📝 Parameters: output={output_path}, sourceobject={sourceobject}, loadtype={loadtype}")

    # Get project root from environment variable
    PROJECT_ROOT = os.getenv('PROJECT_ROOT', r'C:\Users\HP\uber_project')
    logger.info(f"📦 Project root: {PROJECT_ROOT}")

    # Build paths using project root
    DATA_ROOT = os.path.join(PROJECT_ROOT, 'Data')
    logger.info(f"🗃️ Data root: {DATA_ROOT}")

    try:
        setVEnv()
        spark = create_spark_session()
        '''
        Fact Tables - uberfares , tripdetails
        '''
        spark.sparkContext.setLogLevel("INFO")
        logger.info("✅ Spark session initialized successfully")
        sourceschema = sourceschema(sourcedefinition=sourceobject)
        # Your actual data processing code here
        loadio = DataLakeIO(
            process='load',
            sourceobject=sourceobject,
            loadtype=loadtype
        )
        dataloader = DataLoader(
            path=loadio.filepath(),
            schema=sourceschema,
            filetype=loadio.filetype(),
            loadtype=loadtype
        )
        source_data = dataloader.LoadData(spark)
        logger.info("📂 Loading sample data...")

        # Show sample data for verification
        logger.info("📊 Sample data:")
        source_data.show(5, truncate=False)
        datacleaner = DataCleaner(
            sourcedefinition=sourceobject,
            spark=spark,
            loadtype=loadtype
        )
        destination_data = datacleaner.cleandata(sourcedata=source_data)
        currentio = DataLakeIO(
            process="write",
            sourceobject=sourceobject,
            state='current'
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
        logger.info(f"🏁 Processing completed at {datetime.datetime.now()}")
        return 0

    except Exception as e:
        logger.exception(f"🔥 Critical error: {str(e)}")
        return 1
if __name__ == "__main__":
    # Force immediate output flushing
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--sourceobject", required=True)
    parser.add_argument("--loadtype", required=True)

    args = parser.parse_args()
    exit_code = main(args.output, args.sourceobject, args.loadtype)
    sys.exit(exit_code)