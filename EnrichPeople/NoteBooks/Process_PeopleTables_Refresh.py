from EnrichPeople.Config import config,layer , keys, updateitems
from Shared.sparkconfig import create_spark_session_large
from Shared.FileIO import SourceObjectAssignment , DataLakeIO , MergeIO
from Shared.DataWriter import DataWriter
from EnrichPeople.Harmonization import Harmonizer
from Shared.pyspark_env import setVEnv , stop_spark
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(table: str, loadtype: str = 'full', runtype: str = 'dev', initial_load: str = 'no'):
    logging = Logger(notebook_name='Process_PeopleTables_Refresh')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting Peoples Table Data Processing")
    logger.info(f"Parameters:, sourceobject={table}, loadtype={loadtype}")
    try:
        setVEnv()
        logger.info(" Spark session initialized successfully")
        spark = create_spark_session_large()
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
        logger.info("Loaded data...")
        harmonizer = Harmonizer(
            table=table,
            loadtype=loadtype,
            runtype=runtype
        )
        currentio = DataLakeIO(
            process='write',
            table=table,
            state='current',
            layer=layer.get(table),
            loadtype=loadtype,
            runtype=runtype
        )
        destination_data = harmonizer.harmonize(
            spark=spark,
            dataframes=dataframes,
            currentio=currentio
        )

        datawriter = DataWriter(
            loadtype=loadtype,
            path=currentio.filepath(),
            spark=spark,
            format='delta'
        )

        if initial_load == 'yes':
            datawriter.WriteData(df=destination_data)
        else:
            datawriter.WriteParquet(df=destination_data)
            mergeconfig = MergeIO(
                table=table,
                currentio= currentio,
                key_columns= keys.get(table)
            )
            mergeconfig.merge(spark=spark,updated_df=destination_data)

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
    parser.add_argument("--loadtype", required=False , default='full')
    parser.add_argument("--runtype", required=True)
    parser.add_argument("--initial_load", required=False , default='no')

    args = parser.parse_args()
    exit_code = main(args.sourceobject, args.loadtype,args.runtype, args.initial_load)
    sys.exit(exit_code)