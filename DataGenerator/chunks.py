import os
import pandas as pd
import random
from datetime import datetime, timedelta

# Load your data into a DataFrame
df = pd.read_csv('D:/Downloads/nyc_taxi_data_2lakh_rows.csv')

basepath = 'C:/Users/HP/uber_project/Data/UberFaresData/'
start_row = 0
chunk_number = 1
c = 0

while start_row < len(df):
    # pick a random chunk size
    chunk_size = random.randint(5000, 6000)
    end_row = min(start_row + chunk_size, len(df))
    chunk = df.iloc[start_row:end_row]

    # build your date‐folder path
    date = datetime.now() + timedelta(days=c)
    datefolder = date.strftime('%Y-%m-%d')
    folder_path = os.path.join(basepath, datefolder)

    # here: create the directory (and any parents) if it doesn't exist
    os.makedirs(folder_path, exist_ok=True)

    # now save
    out_file = os.path.join(folder_path, 'uberfares.csv')
    chunk.to_csv(out_file, index=False)
    print(f'Saved chunk {chunk_number} to {out_file}')

    start_row = end_row
    chunk_number += 1
    c += 1

print('All chunks saved successfully.')
