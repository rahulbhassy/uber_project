import os

# Set the PYSPARK_PYTHON environment variable
def setEnv():
    os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

def setVEnv():
    os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\uber_project\.venv\Scripts\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\HP\uber_project\.venv\Scripts\python.exe"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"
