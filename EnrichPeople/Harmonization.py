from typing import List, Optional
from Shared.FileIO import DataLakeIO
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, regexp_replace
from pyspark.sql.window import Window


class CustomerProfileHarmonizer:
    def __init__(self, loadtype: str, runtype: str = 'dev'):
        self.loadtype = 'full'
        self.runtype = runtype

    def harmonize(self, spark: SparkSession, dataframes: dict, currentio: Optional[DataLakeIO]):
        # Clean column names by replacing invalid characters
        def clean_column_name(name):
            return regexp_replace(name, r'[ ,;{}()\n\t=]', '_')

        customerdetails = dataframes.get('customerdetails').withColumn(
            'age',
            F.round(
                F.datediff(
                    F.to_date(F.lit("2016-01-01"), "yyyy-MM-dd"),
                    F.to_date(col("date_of_birth"), "yyyy-MM-dd")
                ) / F.lit(365.25),
                2
            )
        ).withColumn(
            'tenure',
            F.round(
                F.datediff(
                    F.to_date(F.lit("2016-01-01"), "yyyy-MM-dd"),
                    F.to_date(col("registration_date"), "yyyy-MM-dd")
                ) / F.lit(365.25),
                2
            )
        ).select(
            'customer_id',
            'customer_name',
            'email',
            'phone_no',
            'address',
            'date_of_birth',
            'registration_date',
            'customer_type',
            'membership_status',
            'age',
            'tenure'
        )

        # Clean payment_method and trip_status values before pivoting
        tripdetails = dataframes.get('tripdetails').select(
            'trip_id',
            'customer_id',
            clean_column_name('payment_method').alias('payment_method'),
            clean_column_name('trip_status').alias('trip_status'),
            'customer_rating'
        )

        fares = dataframes.get('fares').select(
            'trip_id',
            'distance_km',
            'trip_duration_min',
            'tip_amount',
            'total_fareamount',
            'tip_pct',
        )

        fares_trip_combined = fares.alias('f').join(
            tripdetails.alias('t'),
            on='trip_id',
            how='inner'
        )

        pm_stats = fares_trip_combined.groupBy(
            col('t.customer_id'),
            col('t.payment_method')
        ).agg(
            F.count('*').alias('payment_count'),
            round(F.sum('f.total_fareamount'), 2).alias('total_payment_amount')
        )

        counts_pivot = (
            pm_stats
            .groupBy("customer_id")
            .pivot("payment_method")
            .agg(F.first("payment_count"))  # Use first instead of sum to avoid duplicate aggregation
            .na.fill(0)
        )

        # Clean column names after pivot
        cnt_cols = [c for c in counts_pivot.columns if c != "customer_id"]
        counts_renamed = counts_pivot.select(
            col("customer_id"),
            *[col(c).cast("long").alias(f"cnt_{c.lower()}_payment") for c in cnt_cols]
        )

        totals_pivot = (
            pm_stats
            .groupBy("customer_id")
            .pivot("payment_method")
            .agg(F.first("total_payment_amount"))  # Use first instead of sum
            .na.fill(0.0)
        )

        tot_cols = [c for c in totals_pivot.columns if c != "customer_id"]
        totals_renamed = totals_pivot.select(
            col("customer_id"),
            *[col(c).cast("double").alias(f"total_{c.lower()}_amount") for c in tot_cols]
        )

        pm_summary = counts_renamed.join(totals_renamed, on="customer_id", how="full").na.fill(0)

        # Clean trip_status values before pivoting
        trip_status_clean = tripdetails.withColumn(
            "trip_status_clean",
            clean_column_name(col("trip_status"))
        )

        combined_customer = fares_trip_combined.groupBy(
            col('t.customer_id')
        ).agg(
            round(F.sum('f.total_fareamount'), 2).alias('total_fareamount'),
            round(F.sum('f.tip_amount'), 2).alias('total_tip_amount'),
            round(F.avg('f.tip_pct'), 2).alias('avg_tip_pct'),
            round(F.sum('f.distance_km'), 2).alias('total_distance_km'),
            F.sum('f.trip_duration_min').alias('total_trip_duration_min'),
            F.count('t.trip_id').alias('total_trip_count'),
            round(F.avg('t.customer_rating'), 2).alias('avg_customer_rating')
        ).select(
            col('customer_id'),
            col('total_fareamount'),
            col('total_tip_amount'),
            col('avg_tip_pct'),
            col('total_distance_km'),
            col('total_trip_duration_min'),
            col('total_trip_count'),
            col('avg_customer_rating')
        ).join(
            trip_status_clean.groupBy("customer_id")
            .pivot("trip_status_clean")
            .count()
            .na.fill(0),
            on='customer_id',
            how='inner'
        ).join(
            pm_summary,
            on='customer_id',
            how='inner'
        ).join(
            customerdetails,
            on='customer_id',
            how='inner'
        )

        return combined_customer


class Harmonizer:
    _harmonizer_map = {
        "customerprofile": CustomerProfileHarmonizer,
    }

    def __init__(self, table, loadtype: str, runtype: str = 'dev'):
        self.table = table
        self.loadtype = loadtype
        self.runtype = runtype
        self.harmonizer_class = self._harmonizer_map.get(table)

        if not self.harmonizer_class:
            raise ValueError(f"No harmonizer found for source: {table}")
        self.harmonizer_instance = self.harmonizer_class(
            loadtype=self.loadtype,
            runtype=self.runtype
        )

    def harmonize(self, spark: SparkSession, dataframes: dict, currentio: Optional[DataLakeIO]):
        return self.harmonizer_instance.harmonize(
            spark=spark,
            dataframes=dataframes,
            currentio=currentio
        )