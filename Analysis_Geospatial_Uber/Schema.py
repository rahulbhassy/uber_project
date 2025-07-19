from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType


def geospatial_schema():
    return StructType([
        StructField("type", StringType(), True),
        StructField("features", ArrayType(
            StructType([
                StructField("type", StringType(), True),
                StructField("properties", StructType([
                    StructField("name", StringType(), True),
                    StructField("cartodb_id", IntegerType(), True),
                    StructField("created_at", StringType(), True),  # Could be converted to TimestampType
                    StructField("updated_at", StringType(), True)  # Could be converted to TimestampType
                ]), True),
                StructField("geometry", StructType([
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(
                        ArrayType(
                            ArrayType(
                                ArrayType(DoubleType())
                            )
                        )
                    ), True)
                ]), True)
            ])
        ), True)
    ])
