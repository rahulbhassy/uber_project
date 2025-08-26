from typing import List, Optional
from Shared.FileIO import DataLakeIO
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.functions import col, regexp_replace, round, datediff, to_date, lit

class CustomerProfileHarmonizer:
    def __init__(self, loadtype: str, runtype: str = 'dev'):
        self.loadtype = 'full'
        self.runtype = runtype

    @staticmethod
    def clean_column_name(name):
        return regexp_replace(name, r'[ ,;{}()\n\t=]', '_')

    def harmonize(self, spark: SparkSession, dataframes: dict, currentio: Optional[DataLakeIO]):
        # Clean column names once
        clean_col = self.clean_column_name

        # Calculate age and tenure
        customerdetails = (
            dataframes['customerdetails']
            .withColumn('age', round(datediff(to_date(lit("2016-01-01"), "yyyy-MM-dd"),
                                              to_date(col("date_of_birth"), "yyyy-MM-dd")) / 365.25, 2))
            .withColumn('tenure', round(datediff(to_date(lit("2016-01-01"), "yyyy-MM-dd"),
                                                 to_date(col("registration_date"), "yyyy-MM-dd")) / 365.25, 2))
            .select('customer_id', 'customer_name', 'email', 'phone_no', 'address',
                    'date_of_birth', 'registration_date', 'customer_type',
                    'membership_status', 'age', 'tenure')
        )

        # Clean relevant columns
        tripdetails = (
            dataframes['tripdetails']
            .select('trip_id', 'customer_id',
                    clean_col('payment_method').alias('payment_method'),
                    clean_col('trip_status').alias('trip_status'),
                    clean_col('trip_intention').alias('trip_intention'),
                    'customer_rating')
        )

        fares = (
            dataframes['fares']
            .select('trip_id', 'distance_km', 'trip_duration_min', 'tip_amount',
                    'total_fareamount', 'tip_pct', 'pickup_month', 'pickup_day',
                    clean_col('pickup_period').alias('pickup_period'),
                    'is_weather_extreme')
        )

        # Combine fares and tripdetails
        fares_trip_combined = fares.join(tripdetails, on='trip_id', how='inner')

        # Payment method analysis using pivot
        pm_stats = (
            fares_trip_combined
            .groupBy('customer_id', 'payment_method')
            .agg(F.count('*').alias('payment_count'),
                 round(F.sum('total_fareamount'), 2).alias('total_payment_amount'))
        )

        # Single pivot for payment counts and amounts
        payment_methods_df = pm_stats.select('payment_method').distinct()
        payment_methods = [row.payment_method for row in payment_methods_df.collect()]

        pivot_count_exprs = [
            F.first(F.when(col('payment_method') == method, col('payment_count')), True).alias(f'cnt_{method}_payment')
            for method in payment_methods]
        pivot_amount_exprs = [F.first(F.when(col('payment_method') == method, col('total_payment_amount')), True).alias(
            f'total_{method}_amount')
                              for method in payment_methods]

        pm_summary = (
            pm_stats
            .groupBy('customer_id')
            .agg(*pivot_count_exprs, *pivot_amount_exprs)
            .fillna(0)
        )

        # Combined customer statistics
        combined_customer_stats = (
            fares_trip_combined
            .groupBy('customer_id')
            .agg(round(F.sum('total_fareamount'), 2).alias('total_fareamount'),
                 round(F.sum('tip_amount'), 2).alias('total_tip_amount'),
                 round(F.avg('tip_pct'), 2).alias('avg_tip_pct'),
                 round(F.sum('distance_km'), 2).alias('total_distance_km'),
                 F.sum('trip_duration_min').alias('total_trip_duration_min'),
                 F.count('trip_id').alias('total_trip_count'),
                 round(F.avg('customer_rating'), 2).alias('avg_customer_rating'))
        )

        # Get distinct values for all pivot categories using DataFrame operations
        pivot_categories = ['trip_status', 'trip_intention', 'pickup_period', 'pickup_day', 'pickup_month']
        pivot_expressions = []

        for category in pivot_categories:
            # Get distinct values for this category
            distinct_values_df = fares_trip_combined.select(category).distinct()
            distinct_values = [row[category] for row in distinct_values_df.collect()]

            # Create expressions for each value
            for value in distinct_values:
                expr = F.sum(
                    F.when(col(category) == value, 1).otherwise(0)
                ).alias(f"{category}_{value}_count")
                pivot_expressions.append(expr)

        # Add is_weather_extreme pivot
        weather_values = [True, False]
        for value in weather_values:
            expr = F.sum(
                F.when(col('is_weather_extreme') == value, 1).otherwise(0)
            ).alias(f"weather_extreme_{str(value).lower()}_count")
            pivot_expressions.append(expr)

        # Single aggregation for all pivot counts
        all_pivots = (
            fares_trip_combined
            .groupBy('customer_id')
            .agg(*pivot_expressions)
            .fillna(0)
        )

        # Join all data
        final = customerdetails
        final = final.join(combined_customer_stats, 'customer_id', 'inner')
        final = final.join(pm_summary, 'customer_id', 'left')
        final = final.join(all_pivots, 'customer_id', 'left')

        return final

class CustomerPreferenceHarmonizer:
    def __init__(self, loadtype: str, runtype: str = 'dev'):
        self.loadtype = 'full'
        self.runtype = runtype

    def find_customer_preferences(self,df: DataFrame):
        # Payment method preference
        payment_cols = [c for c in df.columns if c.startswith('cnt_') and 'payment' in c]
        payment_stack_expr = "stack(" + str(len(payment_cols)) + ", " + ", ".join(
            [f"'{c.replace('cnt_', '').replace('_payment', '')}', `{c}`" for c in
             payment_cols]) + ") as (payment_method, count)"
        payment_pref = (
            df.select('customer_id', *payment_cols)
            .select('customer_id', F.expr(payment_stack_expr))
            .withColumn('payment_rank', F.row_number().over(Window.partitionBy('customer_id').orderBy(F.desc('count'))))
            .filter(F.col('payment_rank') == 1)
            .select('customer_id', F.col('payment_method').alias('preferred_payment_method'))
        )

        # Trip intention preference
        intention_cols = [c for c in df.columns if c.startswith('trip_intention_') and c.endswith('_count')]
        intention_stack_expr = "stack(" + str(len(intention_cols)) + ", " + ", ".join(
            [f"'{c.replace('trip_intention_', '').replace('_count', '')}', `{c}`" for c in
             intention_cols]) + ") as (trip_intention, count)"
        intention_pref = (
            df.select('customer_id', *intention_cols)
            .select('customer_id', F.expr(intention_stack_expr))
            .withColumn('intention_rank',
                        F.row_number().over(Window.partitionBy('customer_id').orderBy(F.desc('count'))))
            .filter(F.col('intention_rank') == 1)
            .select('customer_id', F.col('trip_intention').alias('preferred_trip_intention'))
        )

        # Pickup period preference
        period_cols = [c for c in df.columns if c.startswith('pickup_period_') and c.endswith('_count')]
        period_stack_expr = "stack(" + str(len(period_cols)) + ", " + ", ".join(
            [f"'{c.replace('pickup_period_', '').replace('_count', '')}', `{c}`" for c in
             period_cols]) + ") as (pickup_period, count)"
        period_pref = (
            df.select('customer_id', *period_cols)
            .select('customer_id', F.expr(period_stack_expr))
            .withColumn('period_rank', F.row_number().over(Window.partitionBy('customer_id').orderBy(F.desc('count'))))
            .filter(F.col('period_rank') == 1)
            .select('customer_id', F.col('pickup_period').alias('preferred_pickup_period'))
        )

        # Pickup day preference
        day_cols = [c for c in df.columns if c.startswith('pickup_day_') and c.endswith('_count')]
        day_stack_expr = "stack(" + str(len(day_cols)) + ", " + ", ".join(
            [f"'{c.replace('pickup_day_', '').replace('_count', '')}', `{c}`" for c in
             day_cols]) + ") as (pickup_day, count)"
        day_pref = (
            df.select('customer_id', *day_cols)
            .select('customer_id', F.expr(day_stack_expr))
            .withColumn('day_rank', F.row_number().over(Window.partitionBy('customer_id').orderBy(F.desc('count'))))
            .filter(F.col('day_rank') == 1)
            .select('customer_id', F.col('pickup_day').alias('preferred_pickup_day'))
        )

        # Pickup month preference
        month_cols = [c for c in df.columns if c.startswith('pickup_month_') and c.endswith('_count')]
        month_stack_expr = "stack(" + str(len(month_cols)) + ", " + ", ".join(
            [f"'{c.replace('pickup_month_', '').replace('_count', '')}', `{c}`" for c in
             month_cols]) + ") as (pickup_month, count)"
        month_pref = (
            df.select('customer_id', *month_cols)
            .select('customer_id', F.expr(month_stack_expr))
            .withColumn('month_rank', F.row_number().over(Window.partitionBy('customer_id').orderBy(F.desc('count'))))
            .filter(F.col('month_rank') == 1)
            .select('customer_id', F.col('pickup_month').alias('preferred_pickup_month'))
        )


        # Join all preferences
        preferences = (
            payment_pref
            .join(intention_pref, 'customer_id', 'left')
            .join(period_pref, 'customer_id', 'left')
            .join(day_pref, 'customer_id', 'left')
            .join(month_pref, 'customer_id', 'left')
        )

        return preferences

    def harmonize(self, spark: SparkSession, dataframes: dict, currentio: Optional[DataLakeIO]):
        # Clean column names by replacing invalid characters
        def clean_column_name(name):
            return regexp_replace(name, r'[ ,;{}()\n\t=]', '_')

        tripdetails = dataframes.get('tripdetails').select(
            'trip_id',
            'customer_id',
            'driver_rating'
        )
        uberfares = dataframes.get('uberfares').select(
            'trip_id',
            'passenger_count'
        )
        fares = dataframes.get('fares').select(
            'trip_id',
            'pickup_borough',
            'dropoff_borough'
        )
        combined_source = tripdetails.join(
            uberfares, on='trip_id', how='inner'
        ).join(
            fares, on='trip_id', how='inner'
        )

        profile = dataframes.get('customerprofile').select(
            'customer_id',
             'cnt_Digital_Wallet_payment', 'cnt_Corporate_Account_payment', 'cnt_Cash_payment',
             'cnt_Debit_Card_payment', 'cnt_Credit_Card_payment', 'total_Digital_Wallet_amount',
             'total_Corporate_Account_amount', 'total_Cash_amount', 'total_Debit_Card_amount',
             'total_Credit_Card_amount', 'trip_status_Completed_count', 'trip_status_Cancelled_count',
             'trip_status_No_Show_count', 'trip_intention_Medical/Healthcare_count',
             'trip_intention_Event/Entertainment_count', 'trip_intention_Social/Visiting_count',
             'trip_intention_Tourism/Sightseeing_count', 'trip_intention_Delivery_count', 'trip_intention_Other_count',
             'trip_intention_Ride_Pooling/Shared_count', 'trip_intention_Shopping_count', 'trip_intention_Errand_count',
             'trip_intention_Exercise/Outdoor_count', 'trip_intention_Dining_count', 'trip_intention_Religious_count',
             'trip_intention_Commute_count', 'trip_intention_Business_count', 'trip_intention_Leisure_count',
             'trip_intention_Hotel_Transfer_count', 'trip_intention_School/Education_count',
             'trip_intention_Airport_Transfer_count', 'trip_intention_Relocation/Move_count',
             'pickup_period_Morning_Rush_count', 'pickup_period_After_Midnight_count', 'pickup_period_Late_Night_count',
             'pickup_period_Midday_count', 'pickup_period_Early_Morning_count', 'pickup_period_Evening_Rush_count',
             'pickup_period_Night_count', 'pickup_day_Wednesday_count', 'pickup_day_Tuesday_count',
             'pickup_day_Friday_count', 'pickup_day_Thursday_count', 'pickup_day_Saturday_count',
             'pickup_day_Monday_count', 'pickup_day_Sunday_count', 'pickup_month_July_count',
             'pickup_month_November_count', 'pickup_month_February_count', 'pickup_month_January_count',
             'pickup_month_March_count', 'pickup_month_October_count', 'pickup_month_May_count',
             'pickup_month_August_count', 'pickup_month_April_count', 'pickup_month_June_count',
             'pickup_month_December_count', 'pickup_month_September_count', 'weather_extreme_true_count',
             'weather_extreme_false_count'
        )
        preference = self.find_customer_preferences(profile)
        # Calculate preferred pickup borough (mode)
        pickup_borough_pref = (
            combined_source
            .groupBy('customer_id', 'pickup_borough')
            .agg(F.count('*').alias('count'))
            .withColumn('pickup_rank', F.row_number().over(
                Window.partitionBy('customer_id').orderBy(F.desc('count'))
            ))
            .filter(F.col('pickup_rank') == 1)
            .select('customer_id', F.col('pickup_borough').alias('preferred_pickup_borough'))
        )

        # Calculate preferred dropoff borough (mode)
        dropoff_borough_pref = (
            combined_source
            .groupBy('customer_id', 'dropoff_borough')
            .agg(F.count('*').alias('count'))
            .withColumn('dropoff_rank', F.row_number().over(
                Window.partitionBy('customer_id').orderBy(F.desc('count'))
            ))
            .filter(F.col('dropoff_rank') == 1)
            .select('customer_id', F.col('dropoff_borough').alias('preferred_dropoff_borough'))
        )

        # Calculate average passenger count
        avg_passenger_count = (
            combined_source
            .groupBy('customer_id')
            .agg(F.avg('passenger_count').alias('avg_passenger_count'))
            .withColumn('preferred_avg_passenger_count',
                        F.round(F.col('avg_passenger_count')))
            .select('customer_id', 'preferred_avg_passenger_count')
        )

        # Calculate average driver rating
        avg_driver_rating = (
            combined_source
            .groupBy('customer_id')
            .agg(round(F.avg('driver_rating'),2).alias('preferred_avg_driver_rating'))
        )

        # Join all preferences
        return preference.join(
            pickup_borough_pref, 'customer_id', 'left'
        ).join(
            dropoff_borough_pref, 'customer_id', 'left'
        ).join(
            avg_passenger_count, 'customer_id', 'left'
        ).join(
            avg_driver_rating, 'customer_id', 'left'
        )


class Harmonizer:
    _harmonizer_map = {
        "customerprofile": CustomerProfileHarmonizer,
        "customerpreference" : CustomerPreferenceHarmonizer
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