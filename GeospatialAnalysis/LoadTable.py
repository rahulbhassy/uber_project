import pandas as pd
import tkinter as tk
from pandastable import Table
from pyspark.sql import DataFrame as PySparkDataFrame

def LoadTable(df):
    # Check if the input is a PySpark DataFrame
    if isinstance(df, PySparkDataFrame):
        # Convert PySpark DataFrame to Pandas DataFrame
        df = df.toPandas()
    elif not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a PySpark or Pandas DataFrame")

    # Initialize Tkinter root window
    root = tk.Tk()
    frame = tk.Frame(root)
    frame.pack(fill="both", expand=True)

    # Create and display the table
    table = Table(frame, dataframe=df, showtoolbar=True, showstatusbar=True)
    table.show()

    # Start the Tkinter main loop
    root.mainloop()
