import os
import json
from pathlib import Path
from datetime import datetime
from dash import Dash, dash_table, html
import dash_bootstrap_components as dbc
from dash_bootstrap_components import Card, CardBody
from pyspark.sql import DataFrame
from shapely.geometry import Point, Polygon, LineString
from pyspark.sql import SparkSession

# -- CONFIG --
DATALAKE_PREFIX = r"C:\Users\HP\uber_project\Data"

class DataLakeIO:

    _RAW_TABLES = frozenset([
        "uberfares",
        "tripdetails",
        "driverdetails",
        "customerdetails",
        "vehicledetails",
        "features"
    ])
    _INPUT_TABLES = frozenset([
        "borough"
    ])
    _ENRICH_TABLES = frozenset([
        "uberfares",
        "uber"
    ])

    def __init__(self,
        process: str,
        layer: str,
        table: str,
        state: str = 'current',
        runtype: str = 'prod',
        loadtype: str = None
    ):
        self.process = process.lower()
        self.layer = layer.lower()
        self.table = table.lower()
        self.state = state.lower()
        self.runtype = runtype.lower()
        self.loadtype = loadtype

    def _getLayer(self):
        if self.layer == 'raw':
            if self.table == 'features':
                return ['Raw','boroughs','newyork','features']
            else:
                return ['Raw']
        elif self.layer == 'input':
            if self.table == 'borough':
                return ['Input','Borough','NewYork']
        elif self.layer == 'enrich':
            if self.table == 'uber':
                return ['Enrich','Enriched','spatial','newyork']
            else:
                return ['Enrich','Enriched']


    def filetype(self) -> str:

        if self.process == 'load':
            if self.table in self._RAW_TABLES:
                return 'csv'
            elif self.table == 'borough':
                return 'geojson'
        elif self.state == 'current':
            return 'delta'
        elif self.state == 'delta':
            return 'parquet'
        raise ValueError(f"No filetype rule for process '{self.table}'")

    def _build_path_parts(self, base_parts, filename):
        """Helper method to build path parts and join them."""
        parts = [DATALAKE_PREFIX.rstrip(os.sep)] + base_parts
        if self.state:
            parts.append(self.state)
        parts.append(filename)
        return os.path.join(*parts)

    def filepath(self,date: str = None) -> str:
        ext = self.filetype()

        if self.process == 'load':
            if self.table in self._RAW_TABLES:
                today_folder = datetime.now().strftime('%Y-%m-%d')
                if self.loadtype == 'full':
                    return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), "DataSource", "*",f"{self.table}.csv")
                elif self.loadtype == 'delta':
                    if date:
                        return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), "DataSource",date,f"{self.table}.csv")
                    else:
                        return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), "DataSource", today_folder,f"{self.table}.csv")
            else:
                ValueError(f"Can't load this table use read '{self.table}'")
        elif self.process == 'read' :
            if self.state == 'current':
                parts = [self.layer,]

