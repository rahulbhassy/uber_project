from Shared.FileIO import DataLakeIO
from Shared.DataLoader import DataLoader
from Shared.FileIO import SparkTableViewer
from Shared.sparkconfig import create_spark_session
from Shared.pyspark_env import setVEnv

# Cleaner, corrected centroid coordinates for US states (including D.C. & Puerto Rico)
# Source: Google states.csv (June 2024) — each row includes state abbreviation, latitude, longitude, and full name :contentReference[oaicite:2]{index=2}
STATE_COORDS = {
    "Alabama":               (32.318231, -86.902298),
    "Alaska":                (63.588753, -154.493062),
    "Arizona":               (34.048928, -111.093731),
    "Arkansas":              (35.201050, -91.831833),
    "California":            (36.778261, -119.417932),
    "Colorado":              (39.550051, -105.782067),
    "Connecticut":           (41.603221, -73.087749),
    "Delaware":              (38.910832, -75.527670),
    "District of Columbia":  (38.905985, -77.033418),
    "Florida":               (27.664827, -81.515754),
    "Georgia":               (32.157435, -82.907123),
    "Hawaii":                (19.898682, -155.665857),
    "Idaho":                 (44.068202, -114.742041),
    "Illinois":              (40.633125, -89.398528),
    "Indiana":               (40.551217, -85.602364),
    "Iowa":                  (41.878003, -93.097702),
    "Kansas":                (39.011902, -98.484246),
    "Kentucky":              (37.839333, -84.270018),
    "Louisiana":             (31.244823, -92.145024),
    "Maine":                 (45.253783, -69.445469),
    "Maryland":              (39.045755, -76.641271),
    "Massachusetts":         (42.407211, -71.382437),
    "Michigan":              (44.314844, -85.602364),
    "Minnesota":             (46.729553, -94.685900),
    "Mississippi":           (32.354668, -89.398528),
    "Missouri":              (37.964253, -91.831833),
    "Montana":               (46.879682, -110.362566),
    "Nebraska":              (41.492537, -99.901813),
    "Nevada":                (38.802610, -116.419389),
    "New Hampshire":         (43.193852, -71.572395),
    "New Jersey":            (40.058324, -74.405661),
    "New Mexico":            (34.972730, -105.032363),
    "New York":              (43.299428, -74.217933),
    "North Carolina":        (35.759573, -79.019300),
    "North Dakota":          (47.551493, -101.002012),
    "Ohio":                  (40.417287, -82.907123),
    "Oklahoma":              (35.007752, -97.092877),
    "Oregon":                (43.804133, -120.554201),
    "Pennsylvania":          (41.203322, -77.194525),
    "Rhode Island":          (41.580095, -71.477429),
    "South Carolina":        (33.836081, -81.163725),
    "South Dakota":          (43.969515, -99.901813),
    "Tennessee":             (35.517491, -86.580447),
    "Texas":                 (31.968599, -99.901813),
    "Utah":                  (39.320980, -111.093731),
    "Vermont":               (44.558803, -72.577841),
    "Virginia":              (37.431573, -78.656894),
    "Washington":            (47.751074, -120.740139),
    "West Virginia":         (38.597626, -80.454903),
    "Wisconsin":             (43.784440, -88.787868),
    "Wyoming":               (43.075968, -107.290284),
    "Puerto Rico":           (18.220833, -66.590149)
}

# Broadcast it once for your Spark job:

setVEnv()
spark = create_spark_session()
reader = DataLakeIO(
    process='read',
    table='uberfares',
    state='current',
    layer='raw',
    loadtype='full',
)

dataloader = DataLoader(
    path=reader.filepath(),
    filetype='delta',
)
df = dataloader.LoadData(spark=spark)
bc_state_coords = spark.sparkContext.broadcast(STATE_COORDS)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import math

def haversine(lat1, lon1, lat2, lon2):
    # radius of earth in km
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def nearest_state(lat, lon):
    # fallback if coords are null
    if lat is None or lon is None:
        return None

    best_state = None
    best_dist = float("inf")
    for state, (s_lat, s_lon) in bc_state_coords.value.items():
        dist = haversine(lat, lon, s_lat, s_lon)
        if dist < best_dist:
            best_dist = dist
            best_state = state
    return best_state

# Register UDF
nearest_state_udf = udf(nearest_state, StringType())

df2 = df \
  .withColumn("pickup_state",
              nearest_state_udf("pickup_latitude", "pickup_longitude")) \
  .withColumn("dropoff_state",
              nearest_state_udf("dropoff_latitude", "dropoff_longitude"))

df2 = df2.groupBy('pickup_state', 'dropoff_state').count()
view = SparkTableViewer(df=df2)
view.display()