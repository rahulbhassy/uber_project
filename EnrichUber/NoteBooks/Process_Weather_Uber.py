from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.FileIO import DataLakeIO
from Shared.DataWriter import DataWriter
from EnrichUber.Schema import weather_schema
from EnrichUber.Harmonization import PreHarmonizer,Harmonizer
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(uber: str, weather: str, loadtype: str = 'delta', runtype: str = 'prod'):

    logging = Logger(notebook_name='Process_Weather_Uber')
    logger = logging.setup_logger()
    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters:, sourceobject={uber}, sourceobject={weather} loadtype={loadtype}")
    try:
        setEnv()
        spark = create_spark_session()
        logger.info(" Spark session initialized successfully")
        weatherschema = weather_schema


        readio = DataLakeIO(
            process='read',
            table=uber,
            state='current',
            loadtype=loadtype,
            layer='raw',
            runtype=runtype
        )
        reader = DataLoader(
            path=readio.filepath(),
            filetype='delta'
        )
        uberdata = reader.LoadData(spark)
        logger.info(f"Loaded {uber} data...")

        readio = DataLakeIO(
            process='read',
            table=weather,
            state='current',
            loadtype=loadtype,
            layer='raw',
            runtype=runtype
        )
        reader = DataLoader(
            path=readio.filepath(),
            filetype='delta'
        )
        weatherdata = reader.LoadData(spark)
        logger.info(f"Loaded {weather} data...")

        currentio = DataLakeIO(
            process="enrichweather",
            table=uber,
            state='current',
            loadtype=loadtype,
            layer='enrich',
            runtype=runtype
        )

        if loadtype == 'delta':
            preharmonizer = PreHarmonizer(
                currentio=currentio
            )
            uberdata = preharmonizer.preharmonize(sourcedata=uberdata, spark=spark)

        harmonizer = Harmonizer(
            uberdata=uberdata,
            weatherdata=weatherdata,
            schema=weatherschema
        )
        enriched_weather_data = harmonizer.harmonize(spark=spark)


        datawriter = DataWriter(
            loadtype=loadtype,
            path=currentio.filepath(),
            spark=spark
        )
        datawriter.WriteData(df=enriched_weather_data)
        logger.info("Data enriched and written successfully...")
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
    parser.add_argument("--uber", required=True)
    parser.add_argument("--weather", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=False)

    args = parser.parse_args()
    exit_code = main(args.uber,args.weather, args.loadtype,args.runtype)
    sys.exit(exit_code)