from typing import List, Optional
from Shared.FileIO import DataLakeIO
from pyspark.sql import DataFrame, SparkSession


class CustomerProfileHarmonizer:
    def __init__(self,loadtype:str,runtype: str = 'dev'):
        self.loadtype = 'full'
        self.runtype = runtype

    def harmonize(self, spark: SparkSession, dataframes: dict, currentio: Optional[DataLakeIO]):
        customerdetails = dataframes.get('customerdetails')
        tripdetails = dataframes.get('tripdetails')
        fares = dataframes.get('fares')

        return


class Harmonizer:
    _harmonizer_map = {
        "customerprofile" : CustomerProfileHarmonizer,
        "driverprofile" : DriverProfileHarmonizer,
        "customerpreference" : CustomerPreferenceHarmonizer,
        "driverperformance" : DriverPerformanceHarmonizer,
        "vehicleperformance" : VehiclePerformanceHarmonizer,
    }

    def __init__(self,table,loadtype: str,runtype: str = 'dev'):
        self.table = table
        self.loadtype = loadtype
        self.runtype = runtype
        self.harmonizer_class = self._harmonizer_map.get(table)

        if not self.harmonizer_class:
            raise ValueError(f"No harmonizer found for source: {table}")
        self.harmonizer_instance = self.harmonizer_class(
            loadtype=self.loadtype,
            runtype=self.runtype
        )

    def harmonize(self,spark: SparkSession ,dataframes: dict, currentio: Optional[DataLakeIO]):
        """Instance method to harmonize data using the selected harmonizer"""
        return self.harmonizer_instance.harmonize(
            spark=spark,
            dataframes=dataframes,
            currentio=currentio
        )