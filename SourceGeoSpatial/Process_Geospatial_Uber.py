from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_sedona
from Shared.FileIO import DataLakeIO, GeoJsonIO
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Schema import geospatial_schema
from GeospatialVH import Validator,Harmonizer

'''
input_file_names = [
    'connecticut_municipalities.geojson',
    'massachusetts_municipalities.geojson',
    'new-york-city-boroughs.geojson',
    'new_jersey_municipalities.geojson',
    'pennsylvania_municipalities.geojson',
    'rhode_island_municipalities.geojson',
    'quebec_municipalities.geojson',
    'ontario_municipalities.geojson',
    'connecticut_outside_boroughs.geojson',
    'massachusetts_outside_boroughs.geojson',
    'new_jersey_outside_boroughs.geojson',
    'new_york_outside_boroughs.geojson',
    'ontario_outside_boroughs.geojson',
    'pennsylvania_outside_boroughs.geojson',
    'quebec_outside_boroughs.geojson',
    'rhode_island_outside_boroughs.geojson',
    'vermont_outside_boroughs.geojson'
]
'''
setVEnv()
spark = create_spark_session_sedona()
skip_validation = False
input_file_names = [
    'connecticut_municipalities.geojson',
    'massachusetts_municipalities.geojson',
    'new-york-city-boroughs.geojson',
    'new_jersey_municipalities.geojson',
    'pennsylvania_municipalities.geojson',
    'rhode_island_municipalities.geojson',
    'quebec_municipalities.geojson',
    'connecticut_outside_boroughs.geojson',
    'massachusetts_outside_boroughs.geojson',
    'new_jersey_outside_boroughs.geojson',
    'new_york_outside_boroughs.geojson',
    'ontario_outside_boroughs.geojson',
    'pennsylvania_outside_boroughs.geojson',
    'quebec_outside_boroughs.geojson',
    'rhode_island_outside_boroughs.geojson',
    'vermont_outside_boroughs.geojson'
]
loadtype='delta'

for input_file_name in input_file_names:

    validator = Validator()
    harmonizer = Harmonizer(
        validator=validator
    )

    if not skip_validation:
        loadio = DataLakeIO(
            process='load',
            table=input_file_name,
            loadtype=loadtype,
            layer= 'input'
        )
        geojsonio = GeoJsonIO(
            input_filename=input_file_name,
            path=loadio.filepath()
        )
        input_file_name = geojsonio.validate_and_fix_geojson(
            validator_func=validator.geojsonvalidator
        )

    readio = DataLakeIO(
        process='read',
        table=input_file_name,
        layer = 'input',
        loadtype=loadtype
    )
    dataloader = DataLoader(
        path=readio.filepath(),
        schema=geospatial_schema(),
        filetype='geojson'
    )
    boroughs_df = dataloader.LoadData(spark)

    features_df = harmonizer.harmoniseboroughs(boroughs_df=boroughs_df)
    currentio = DataLakeIO(
        process='write',
        table='features',
        state= 'current',
        loadtype= loadtype,
        layer='raw'
    )
    datawriter = DataWriter(
        loadtype=loadtype,
        path=currentio.filepath(),
        format=currentio.file_ext(),
        spark=spark
    )
    datawriter.WriteData(df=features_df)

spark.stop()
