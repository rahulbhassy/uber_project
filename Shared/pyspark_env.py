
import os
import gc
from pyspark.sql import SparkSession

# Set the PYSPARK_PYTHON environment variable
def setEnv():
    os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python310\python.exe"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

def setVEnv():
    os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\uber_project\.venv\Scripts\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\HP\uber_project\.venv\Scripts\python.exe"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

def stop_spark(spark=None):
    """
    Clean shutdown:
      - cancel jobs
      - stop session/context
      - shutdown py4j gateway
      - optional: kill leftover java child processes (last resort)
    """
    try:
        # get active if not provided
        if spark is None:
            spark = SparkSession.getActiveSession()
    except Exception:
        spark = None

    try:
        if spark is not None:
            # cancel running jobs first
            try:
                spark.sparkContext.cancelAllJobs()
            except Exception:
                pass
            # stop spark session / context
            try:
                spark.stop()
            except Exception:
                pass
    except Exception:
        pass

    # try to shutdown py4j gateway explicitly
    try:
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        if sc is not None and getattr(sc, "_gateway", None) is not None:
            try:
                sc._gateway.shutdown()
            except Exception:
                pass
    except Exception:
        pass

    # python GC
    try:
        gc.collect()
    except Exception:
        pass
