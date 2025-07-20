from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType
)

def WeatherSchema() -> StructType:
    return StructType([
        StructField("date", StringType(), nullable=False),        # accept as string
        StructField("avg_temp", DoubleType(), nullable=True),
        StructField("precipitation", DoubleType(), nullable=True),
    ])
