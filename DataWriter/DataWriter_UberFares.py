from DataCleaning.DataCleaning_UberFares import Clean_Add_Temporal_Features

def process_data():
    processed_data = Clean_Add_Temporal_Features()
    processed_data.write \
        .format("delta") \
        .mode("overwrite") \
        .save("C:/Users/HP/uber_project/Data/Cleaned_UberFares/UberFares.csv")

    return processed_data

df = process_data()
df.show()
df.count()

