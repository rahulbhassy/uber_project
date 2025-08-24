from typing import List
from Shared.pyspark_env import setVEnv , stop_spark
from Shared.sparkconfig import create_spark_session
from Shared.FileIO import SourceObjectAssignment
from Shared.FileIO import DataLakeIO
from Balancing.config import CHECKS, SCHEMA , layer
from Balancing.utilities import Balancing
from Shared.DataWriter import DataWriter
from Shared.Logger import Logger
import argparse
import sys
import datetime

def main(runtype: str = 'dev', loadtype: str = 'delta', tables: List[str] = 'all'):
    logging = Logger(notebook_name='Process_Balancing')
    logger = logging.setup_logger()

    # Log critical environment information
    logger.info(f"Starting UberFares Data Processing")
    logger.info(f"Parameters:, tables={tables}, loadtype={loadtype}")
    try:
        setVEnv()
        spark = create_spark_session()
        logger.info("Spark session initialized successfully")
        tables = CHECKS.keys() if tables[0] == 'all' else tables
        final = spark.createDataFrame([], SCHEMA)
        results = []
        for table_name in tables:
            sourcetable_assignment = SourceObjectAssignment(
                sourcetables=CHECKS[table_name]['tables'],
                loadtype=loadtype,
                runtype=runtype
            )
            io_map = {}
            if layer.get(table_name) == 'raw':
                io_map = sourcetable_assignment.assign_DataLakeIO(layer=layer, process='load')
            else:
                io_map = sourcetable_assignment.assign_DataLakeIO(layer=layer)

            targetio = DataLakeIO(
                process='read',
                table=table_name,
                state='current',
                layer=layer.get(table_name),
                loadtype=loadtype,
                runtype=runtype
            )
            target_mapping = {table_name: targetio.filepath()}
            targetquery = CHECKS[table_name]['targetquery'].format(**target_mapping)

            source_mapping = {t: io_map[t].filepath() for t in CHECKS[table_name]['tables']}
            sourcequery = CHECKS[table_name]['sourcequery'].format(**source_mapping)

            balancing = Balancing(
                table=table_name,
                sourcequery=sourcequery,
                targetquery=targetquery,
            )

            df, result = balancing.getResult(spark=spark)
            final = final.unionByName(df)
            results.append(result)

        balancingio = DataLakeIO(
            process='write',
            table='balancingresults',
            state='current',
            layer='system',
            loadtype=loadtype,
            runtype=runtype
        )

        writer = DataWriter(
            loadtype='delta',
            path=balancingio.filepath(),
            format='delta',
            spark=spark
        )

        writer.WriteData(df=final)
        if 'Fail' in results:
            raise Exception("Balancing checks failed. Please review the logs for details.")

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
    parser.add_argument("--runtype", required=False)
    parser.add_argument("--loadtype", required=True)
    parser.add_argument("--tables", nargs='+', default=['all'], help="List of tables to process, or 'all' for all tables")

    args = parser.parse_args()
    exit_code = main(args.loadtype,args.runtype,args.tables)
    sys.exit(exit_code)