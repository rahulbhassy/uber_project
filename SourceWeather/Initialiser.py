from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from typing import Optional


class Init:
    def __init__(
        self,
        loadtype: str,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
        date_format: str = '%Y-%m-%d'
    ):
        self.loadtype = loadtype.lower()
        self.date_format = date_format

        # Parse string dates to datetime.date
        if startdate:
            try:
                self.startdate = datetime.strptime(startdate, self.date_format).date()
            except ValueError:
                raise ValueError(f"startdate must match format {self.date_format}")
        else:
            self.startdate = None

        if enddate:
            try:
                self.enddate = datetime.strptime(enddate, self.date_format).date()
            except ValueError:
                raise ValueError(f"enddate must match format {self.date_format}")
        else:
            self.enddate = None

    def Load(self, spark: SparkSession) -> DataFrame:
        """
        Create a DataFrame with a date range for the specified load type.

        :param spark: SparkSession instance
        :return: DataFrame with date range
        """
        print("\n" + "=" * 80)
        print("INITIALISING DATE RANGE")
        print("=" * 80)

        today_str = datetime.now().strftime(self.date_format)
        yesterday_date = datetime.now().date() - timedelta(days=1)
        yesterday_str = yesterday_date.strftime(self.date_format)

        if self.loadtype == 'full':
            # Historical full load requires both start and end
            if not self.startdate or not self.enddate:
                raise ValueError("Start and end dates must be provided for historical data.")
            start_str = self.startdate.strftime(self.date_format)
            end_str = self.enddate.strftime(self.date_format)
            print(f" Creating date range from {start_str} to {end_str}")
            num_days = (self.enddate - self.startdate).days + 1
            date_range = (
                spark.range(num_days)
                     .selectExpr(f"date_add('{start_str}', cast(id as int)) as date")
            )
        else:
            # Incremental load: from startdate through yesterday
            if self.startdate:
                start_str = self.startdate.strftime(self.date_format)
                if self.startdate > yesterday_date:
                    raise ValueError(
                        f"Start date {start_str} is after yesterday {yesterday_str}."
                    )
                print(f" Creating date range from {start_str} to {yesterday_str}")
                num_days = (yesterday_date - self.startdate).days + 1
                date_range = (
                    spark.range(num_days)
                         .selectExpr(f"date_add('{start_str}', cast(id as int)) as date")
                )
            else:
                # If no startdate, default to yesterday only
                print(f" No startdate provided; defaulting to yesterday {yesterday_str}")
                date_range = spark.createDataFrame(
                    [(yesterday_str,)],
                    ["date"]
                )

        return date_range
