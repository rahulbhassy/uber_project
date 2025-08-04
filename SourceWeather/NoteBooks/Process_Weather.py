from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session
from SourceWeather.Initialiser import Init
from SourceWeather.Schema import weather_schema
from SourceWeather.APILoader import WeatherAPI
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(table, loadtype,runtype='prod'):
    from SourceUberFact.Schema import sourceschema
    logging = Logger(notebook_name='Process_Weather')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters:, sourceobject={table}, loadtype={loadtype}")

    try:
        setVEnv()
        spark = create_spark_session()
        init = Init(
            loadtype=loadtype,
            spark=spark,
            table=table
        )
        df = init.Load()

        dataloader = WeatherAPI(schema=weather_schema)
        weatherdetails = dataloader.load(
            data=df,
            spark=spark
        )

        currentio = DataLakeIO(
            process='write',
            table=table,
            state='current',
            layer='raw',
            loadtype=loadtype,
            runtype='runtype'
        )
        writer = DataWriter(
            loadtype=loadtype,
            path=currentio.filepath(),
            spark=spark
        )
        writer.WriteData(df=weatherdetails)
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
    parser.add_argument("--table", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=True)


    args = parser.parse_args()
    exit_code = main(args.table, args.loadtype,args.runtype)
    sys.exit(exit_code)