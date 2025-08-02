from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from typing import Optional
from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader


class Init:
    def __init__(
        self,
        loadtype: str,
        spark: SparkSession,
        table:  str,
        date_format: str = '%Y-%m-%d'
    ):
        self.loadtype = loadtype.lower()
        self.spark = spark
        self.table = table
        self.date_format = date_format

        if self.loadtype == 'full':
            self.startdate = datetime.strptime('2009-01-01',self.date_format).date()
        else:
            readio = DataLakeIO(
                process='read',
                table=self.table,
                state='current',
                layer='raw',
                loadtype=loadtype
            )
            reader = DataLoader(
                path=readio.filepath(),
                filetype='delta',
                loadtype=loadtype
            )
            self.startdate = (
                reader
                .LoadData(spark=self.spark)
                .selectExpr("max(date) AS max_date")
                .first()["max_date"]
            ) + timedelta(days=1)

        self.enddate = datetime.now().date() - timedelta(days=1)

    def Load(self) -> DataFrame:
        """
        Create a DataFrame with a date range for the specified load type.

        :param spark: SparkSession instance
        :return: DataFrame with date range
        """
        print("\n" + "=" * 80)
        print("INITIALISING DATE RANGE")
        print("=" * 80)

        # Historical full load requires both start and end
        if not self.startdate or not self.enddate:
            raise ValueError("Start and end dates must be provided for historical data.")
        start_str = self.startdate.strftime(self.date_format)
        end_str = self.enddate.strftime(self.date_format)
        print(f" Creating date range from {start_str} to {end_str}")
        num_days = (self.enddate - self.startdate).days + 1
        date_range = (
            self.spark.range(num_days)
                .selectExpr(f"date_add('{start_str}', cast(id as int)) as date")
        )

        return date_range
