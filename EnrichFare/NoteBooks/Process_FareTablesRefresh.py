from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session , create_spark_session_jdbc
from Shared.DataWriter import DataWriter
from Shared.FileIO import DataLakeIO , SourceObjectAssignment
from EnrichFare.Harmonization import Harmonizer
from EnrichFare.Config import config, layer
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(table: str, loadtype: str, runtype: str ='dev'):

    logging = Logger(notebook_name='Process_FareTablesRefresh')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters:, table={table}, loadtype={loadtype}")
    setVEnv()

    try:
        spark = create_spark_session_jdbc() if table == 'timeseries' else create_spark_session()
        logger.info(" Spark session initialized successfully")
        sourcetables = config[table]
        sourceobjectassignments = SourceObjectAssignment(
            sourcetables=sourcetables,
            loadtype=loadtype,
            runtype=runtype
        )
        sourcereaders = sourceobjectassignments.assign_Readers(
            io_map=sourceobjectassignments.assign_DataLakeIO(layer=layer)
        )
        dataframes = sourceobjectassignments.getData(
            spark=spark,
            readers=sourcereaders
        )
        logger.info("Loaded Source data...")
        currentio = DataLakeIO(
            process='write',
            table=table,
            state='current',
            layer=layer.get(table),
            loadtype=loadtype,
            runtype=runtype
        )
        harmonizer = Harmonizer(
            table=table,
            loadtype=loadtype,
            runtype=runtype
        )
        destination_data = harmonizer.harmonize(
            spark=spark,
            dataframes=dataframes,
            currentio=currentio
        )
        logger.info("Data harmonized...")

        datawriter = DataWriter(
            loadtype=loadtype,
            path=currentio.filepath(),
            format='delta',
            spark=spark
        )
        datawriter.WriteData(df=destination_data)
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
    parser.add_argument("--table", required=True)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--runtype", required=False)

    args = parser.parse_args()
    exit_code = main(args.sourceobject, args.loadtype,args.runtype)
    sys.exit(exit_code)