
def EnrichGeoSpatialData(data):
    data.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/Users/HP/uber_project/Data/EnrichedGeoSpatial/UberFares.csv")

    return data


