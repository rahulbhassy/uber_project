from sedona.spark import SedonaContext
from Shared.sparkconfig import create_spark_session_sedona
from Shared.pyspark_env import setEnv
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO
from pyspark.sql.functions import expr, col
from EnrichGeoSpatial.Harmonization import PreHarmonizer,Harmonizer
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(uber:str,borough: str,trip:str,loadtype: str, runtype: str = 'prod'):
    logging = Logger(notebook_name='Process_Weather_Uber')
    logger = logging.setup_logger()
    try:

        # Log critical environment information
        logger.info(f"Starting UberFares Data Processing")
        logger.info(f"Parameters:, sourceobject={uber}, sourceobject={borough} , {trip}, loadtype={loadtype}")
        setEnv()
        spark = create_spark_session_sedona()
        SedonaContext.create(spark)
        sourceobjectuber = uber
        sourceobjectborough = borough
        sourceobjecttrip = trip
        loadtype = loadtype

        readuberio = DataLakeIO(
            process='read',
            table=sourceobjectuber,
            state='current',
            loadtype=loadtype,
            layer='enrich',
            runtype=runtype
        )
        reader = DataLoader(
            path=readuberio.filepath(),
            filetype=readuberio.file_ext(),
            loadtype=loadtype
        )
        currentio = DataLakeIO(
            process='write',
            table='uber',
            state='current',
            layer='enrich',
            loadtype='full',
            runtype=runtype
        )
        preharmonizer = PreHarmonizer(
            sourcedata=reader.LoadData(spark=spark),
            currentio=currentio,
            loadtype=loadtype
        )
        uber_df = preharmonizer.preharmonize(spark=spark)

        # 3. Load boroughs data
        readfeaturesio = DataLakeIO(
            process='read',
            table=sourceobjectborough,
            state='current',
            layer='raw',
            loadtype='full',
            runtype=runtype
        )
        dataloader = DataLoader(
            path=readfeaturesio.filepath(),
            filetype=readfeaturesio.file_ext(),
            loadtype=loadtype
        )
        featuresdata = dataloader.LoadData(spark=spark)

        boroughs_df = featuresdata.withColumn(
            "geom",
            expr("ST_GeomFromGeoJSON(geometry_json)")
        ).select("borough", "geom")


        tripio = DataLakeIO(
            process='read',
            table=sourceobjecttrip,
            state='current',
            layer='raw',
            loadtype='full',
            runtype=runtype
        )
        harmonizer = Harmonizer(
            sourcedata=uber_df,
            boroughs_df=boroughs_df,
            tripio=tripio
        )
        enriched_uber = harmonizer.harmonize(spark=spark)
        # 7. Save results
        datawriter = DataWriter(
            loadtype=loadtype,
            path=currentio.filepath(),
            format=currentio.file_ext(),
            spark=spark
        )
        datawriter.WriteData(df=enriched_uber)
        spark.stop()
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
    parser.add_argument("--borough", required=True)
    parser.add_argument("--trip", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=False)

    args = parser.parse_args()
    exit_code = main(
        uber=args.uber,
        borough=args.borough,
        loadtype=args.loadtype,
        runtype=args.runtype
    )
    sys.exit(exit_code)
