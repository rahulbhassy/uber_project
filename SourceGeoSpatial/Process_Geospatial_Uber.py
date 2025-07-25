from Shared.pyspark_env import setVEnv
from Shared.sparkconfig import create_spark_session_sedona
from Shared.FileIO import DataLakeIO, GeoJsonIO
from Shared.DataLoader import DataLoader
from Shared.DataWriter import DataWriter
from Schema import geospatial_schema
from GeospatialVH import Validator,Harmonizer

setVEnv()
spark = create_spark_session_sedona()
skip_validation = True
input_file_name = 'new-york-city-boroughs.geojson'
loadtype='full'
validator = Validator()
harmonizer = Harmonizer(
    validator=validator
)

if not skip_validation:
    loadio = DataLakeIO(
        process='load',
        sourceobject=input_file_name
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
    sourceobject=input_file_name
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
    sourceobject='features',
    state= 'current'
)
datawriter = DataWriter(
    loadtype=loadtype,
    path=currentio.filepath(),
    format=currentio.filetype(),
    spark=spark
)
datawriter.WriteData(df=features_df)

spark.stop()
