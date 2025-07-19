import json
import geojson_validator
from geojson_rewind import rewind
import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, to_json

class Validator:
    def geojsonvalidator(self, geojson_input):
        """Validate and fix the GeoJSON structure and geometries."""
        geojson_validator.validate_structure(geojson_input)
        geojson_validator.validate_geometries(geojson_input)
        fixed_geojson = geojson_validator.fix_geometries(geojson_input)
        fixed_geojson = rewind(fixed_geojson)
        return fixed_geojson

    def get_fix_geometry_udf(self):
        validator_func = self.geojsonvalidator

        @pandas_udf(StringType())
        def fix_geometry_pandas(geometry_series: pd.Series) -> pd.Series:
            """Pandas UDF to fix geometries in batch processing without adding extra properties."""

            def validate_and_fix(geo):
                geojson_obj = json.loads(geo)
                fixed_geojson = validator_func(geojson_obj)
                geometry = fixed_geojson.get("geometry", {})
                return json.dumps(geometry) if geometry else geo

            return geometry_series.apply(lambda geo: validate_and_fix(geo) if geo else geo)

        return fix_geometry_pandas


class Harmonizer:
    def __init__(self, validator: Validator):
        self.validator = validator

    def harmoniseboroughs(self, boroughs_df: DataFrame):
        features_df = boroughs_df.select(explode("features").alias("feature"))
        features_df = features_df.select(
            col("feature.properties.name").alias("borough"),
            col("feature.geometry").alias("geometry")
        )
        features_df = features_df.withColumn("geometry_json", to_json("geometry"))

        # Get the UDF from the validator and apply
        fix_geometry_udf = self.validator.get_fix_geometry_udf()
        return features_df.withColumn("geometry_json", fix_geometry_udf("geometry_json"))
