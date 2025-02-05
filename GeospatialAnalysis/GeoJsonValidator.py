import json
import geojson_validator
from geojson_rewind import rewind
import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType


def geojsonvalidator(geojson_input):
    """Validate and fix the GeoJSON structure and geometries."""
    geojson_validator.validate_structure(geojson_input)
    geojson_validator.validate_geometries(geojson_input)
    fixed_geojson = geojson_validator.fix_geometries(geojson_input)
    fixed_geojson = rewind(fixed_geojson)
    return fixed_geojson


@pandas_udf(StringType())
def fix_geometry_pandas(geometry_series: pd.Series) -> pd.Series:
    """Pandas UDF to fix geometries in batch processing without adding extra properties."""

    def validate_and_fix(geo):
        geojson_obj = json.loads(geo)
        fixed_geojson = geojsonvalidator(geojson_obj)
        geometry = fixed_geojson.get("geometry", {})
        return json.dumps(geometry) if geometry else geo

    return geometry_series.apply(lambda geo: validate_and_fix(geo) if geo else geo)


'''# Load the GeoJSON file
geojson_path = "C:/Users/HP/uber_project/Data/Borough/new-york-city-boroughs.geojson"
with open(geojson_path, 'r') as file:
    geojson_input = json.load(file)

fixed_geojson_path = "C:/Users/HP/uber_project/Data/Borough/fixed_new-york-city-boroughs.geojson"
fixed_geojson = geojsonvalidator(geojson_input)

with open(fixed_geojson_path, 'w') as file:
    json.dump(fixed_geojson, file, indent=4)

print("GeoJSON validation and fixing completed successfully.")
'''

