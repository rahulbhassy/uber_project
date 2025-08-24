from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setVEnv , stop_spark
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from EnrichUber.Harmonization import Distance,PreHarmonizer
from Shared.Logger import Logger
import argparse
import sys
import datetime


def main(table: str,loadtype: str, runtype: str = 'prod'):

    logging = Logger(notebook_name='Process_Distance_Uber')
    logger = logging.setup_logger()
    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters:, sourceobject={table}, loadtype={loadtype}")
    try:
        setVEnv()
        spark = create_spark_session()
        logger.info(" Spark session initialized successfully")
        readio = DataLakeIO(
            process="enrichweather",
            table=table,
            state='current',
            layer='enrich',
            loadtype=loadtype,
            runtype=runtype
        )
        dataloader = DataLoader(
            path=readio.filepath(),
            filetype='delta'
        )
        enriched_weather_data = dataloader.LoadData(spark)
        logger.info("Loaded data...")
        currentio = DataLakeIO(
            process="write",
            table=table,
            state='current',
            layer='enrich',
            loadtype=loadtype,
            runtype=runtype
        )

        if loadtype == 'delta':
            preharmonizer = PreHarmonizer(
                currentio=currentio
            )
            enriched_weather_data=preharmonizer.preharmonize(
                sourcedata=enriched_weather_data,
                spark=spark
            )


        enrichdistance = Distance()
        enriched_data = enrichdistance.enrich(data=enriched_weather_data)

        logger.info("Data cleaned...")
        datawriter = DataWriter(
            loadtype=loadtype,
            path=currentio.filepath(),
            spark=spark
        )
        datawriter.WriteData(df=enriched_data)

        stop_spark(spark=spark)
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
    parser.add_argument("--runtype", required=False)

    args = parser.parse_args()
    exit_code = main(args.table, args.loadtype,args.runtype)
    sys.exit(exit_code)