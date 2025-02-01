from DataCleaning.DataCleaning_UberFares import add_temporal_features

processed_data = add_temporal_features()

processed_data.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/Users/HP/uber_project/Data/Cleaned_UberFares/UberFares.csv")

processed_data.show()