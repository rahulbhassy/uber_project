
import json
from pathlib import Path
from dash import Dash, dash_table, html
import dash_bootstrap_components as dbc
from dash_bootstrap_components import Card, CardBody
from pyspark.sql import DataFrame
from shapely.geometry import Point, Polygon, LineString
from pyspark.sql import SparkSession

# -- CONFIG --
import os
from datetime import datetime
from typing import List, Optional

DATALAKE_PREFIX = r"C:\Users\HP\uber_project\Data"


class DataLakeIO:
    _RAW_TABLES = frozenset({
        "uberfares", "tripdetails", "driverdetails",
        "customerdetails", "vehicledetails", "features"
    })
    _INPUT_SUFFIX = ".geojson"

    # centralized layer‐to‐path mapping
    _LAYER_MAP = {
        'raw': {
            'features': ['Raw', 'boroughs', 'newyork', 'features'],
            '__default__': ['Raw']
        },
        'input': {
            '__default__': ['Input', 'Borough', 'NewYork']
        },
        'enrich': {
            'uber': ['Enrich', 'Enriched', 'spatial', 'newyork'],
            'uberfares|enrichweather': ['Enrich', 'Enriched_Weather_uberData'],
            '__default__': ['Enrich', 'Enriched']
        }
    }

    def __init__(
            self,
            process: str,
            table: str,
            loadtype: str,
            state: Optional[str] = 'current',
            runtype: Optional[str] = 'prod',
            layer: Optional[str] = None,
    ):
        self.process = process.lower()
        if layer:
            self.layer = layer.lower()
        else:
            self.layer = layer
        self.table = table.lower()
        self.state = state.lower()
        self.runtype = runtype.lower()
        self.loadtype = loadtype

    def _get_layer_parts(self) -> List[str]:
        cfg = self._LAYER_MAP.get(self.layer, {})
        # special keys
        if self.layer == 'raw' and self.table == 'features':
            return cfg['features']
        if self.layer == 'enrich' and self.table == 'uber':
            return cfg['uber']
        if self.layer == 'enrich' and self.table == 'uberfares':
            return cfg['uberfares|enrichweather']
        # fallback
        return cfg.get('__default__', [])

    def file_ext(self) -> str:
        if self.process == 'load':
            if self.table in self._RAW_TABLES:
                return 'csv'
            if self.table.endswith(self._INPUT_SUFFIX):
                return 'geojson'
        if self.process in ('read', 'write'):
            return 'delta' if self.state == 'current' else 'parquet'

    def _build_path(self, parts: List[str]) -> str:
        if self.runtype == 'dev':
            return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), 'Sandbox', *parts)
        return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), *parts)

    def filepath(self, date: Optional[str] = None) -> str:
        ext = self.file_ext()

        if self.process == 'load':
            if self.table in self._RAW_TABLES:
                # YYYY‑MM‑DD
                folder = date or datetime.now().strftime('%Y-%m-%d')
                if self.loadtype == 'full':
                    return self._build_path(['DataSource', '*', f"{self.table}.csv"])
                # delta or default
                return self._build_path(['DataSource', folder, f"{self.table}.csv"])

            if self.table.endswith(self._INPUT_SUFFIX):
                parts = self._get_layer_parts() + [self.table]
                return self._build_path(parts)

            raise ValueError(f"Can't load table '{self.table}' in process 'load'")

        if self.process in ('read', 'write'):
            parts = self._get_layer_parts()
            if self.state == 'current':
                parts = parts + [self.table, self.state, f"{self.table}.{ext}"]
            elif self.state == 'delta':
                folder = date or datetime.now().strftime('%Y-%m-%d')
                parts = parts + [self.table, self.state,folder, f"{self.table}.{ext}"]
            return self._build_path(parts)

        raise ValueError(f"Unknown process '{self.process}'")


class GeoJsonIO:
    def __init__(self, input_filename: str, path: str, validator_func=None):
        self.input_filename = input_filename
        self.validator_func = validator_func
        self.path = path

    def load(self):
        """Load GeoJSON data from file."""
        with open(self.path, 'r') as file:
            return json.load(file)

    def validate_and_fix_geojson(self, validator_func=None):
        """
        Loads, validates, and writes back a fixed GeoJSON file.

        :param input_filename: name of the original GeoJSON file
        :param output_filename: name of the fixed output GeoJSON file
        :param validator_func: a callable that accepts GeoJSON dict and returns a fixed version
        """
        # Use provided validator_func or fall back to instance variable
        validator = validator_func or self.validator_func
        if validator is None:
            raise ValueError("A `validator_func` must be provided to validate and fix the GeoJSON.")

        # Load, validate, and write fixed GeoJSON
        geojson_input = self.load()
        fixed_geojson = validator(geojson_input)

        output_filename = f"fixed_{self.input_filename}"
        datalake_io = DataLakeIO(process='write', table=output_filename,loadtype='full')
        fixed_geojson_path = datalake_io.filepath()

        with open(fixed_geojson_path, 'w') as file:
            json.dump(fixed_geojson, file, indent=4)

        print("GeoJSON validation and fixing completed successfully.")
        return output_filename

class DeltaLakeOps:
    def __init__(self,path: str,spark: SparkSession):
        self.path = path
        self.spark = spark

    def getHistory(self,count: bool = False):
        historyDF = self.spark.sql(f"DESCRIBE HISTORY delta.`{self.path}`")
        historyDF.show()
        if count:
            # 2. Collect just the version numbers
            versions = [row.version for row in historyDF.select("version").collect()]

            # 3. For each version, issue a COUNT(*) query
            results = []
            for v in versions:
                cnt = self.spark.sql(f"""
                    SELECT {v} AS version,
                           COUNT(*) AS record_count
                      FROM delta.`{self.path}` VERSION AS OF {v}
                """).collect()[0]
                results.append((cnt.version, cnt.record_count))

            # 4. Display all at once
            for version, count in results:
                print(f"Version {version:>2} → {count} rows")

    def restore(self, version: int):
        """
        Restores the Delta table to a specific version.

        :param version: The version number to restore to.
        """
        self.spark.sql(f"""
            RESTORE delta.`{self.path}`
            TO VERSION AS OF {version};
        """)
        print(f"Restored Delta table at {self.path} to version {version}.")


class SparkTableViewer:
    """
    Dash-based Spark DataFrame viewer.
    If table name is 'spatial', handles geometry serialization.
    """

    def __init__(self, df: DataFrame, table_name: str = '', limit: int = 500, page_size: int = 20):
        if not hasattr(df, 'columns'):
            raise ValueError("Expected a PySpark DataFrame.")

        self.df = df.limit(limit)
        self.columns = df.columns
        self.page_size = page_size
        self.table_name = table_name.lower()

        # Choose the appropriate serialization
        if self.table_name == 'spatial':
            self.data = self._serialize_spatial_rows(self.df.collect(), self.columns)
        else:
            self.data = [row.asDict() for row in self.df.collect()]

    def _serialize_spatial_value(self, val):
        try:
            if hasattr(val, 'toText'):  # Sedona geometry
                return val.toText()
            elif isinstance(val, (Point, Polygon, LineString)):  # Shapely geometry
                return val.wkt
            return val
        except Exception:
            return str(val)

    def _serialize_spatial_rows(self, rows, columns):
        serialized = []
        for row in rows:
            serialized.append({
                col: self._serialize_spatial_value(val)
                for col, val in zip(columns, row)
            })
        return serialized

    def display(self, host='127.0.0.1', port=8050, debug=True):
        """
        Launch Dash app with styled DataTable inside a Bootstrap card.
        """
        app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

        table = dash_table.DataTable(
            columns=[{"name": c, "id": c} for c in self.columns],
            data=self.data,
            page_size=self.page_size,
            filter_action="native",
            sort_action="native",
            style_table={
                'overflowX': 'auto',
                'overflowY': 'auto',
                'maxHeight': 'calc(100vh - 200px)',
                'height': '100%',
                'width': '100%',
            },
            style_header={
                'backgroundColor': '#004085',
                'color': 'white',
                'fontWeight': 'bold',
                'textAlign': 'center',
                'position': 'sticky',
                'top': 0,
                'zIndex': 1,
            },
            style_cell={
                'textAlign': 'left',
                'padding': '5px',
                'minWidth': '100px',
                'whiteSpace': 'normal',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'}
            ]
        )

        card = Card([
            html.H4("Spark DataFrame Viewer", className="card-title p-2 text-white bg-primary"),
            CardBody(table)
        ], className="m-4 shadow-sm")

        app.layout = html.Div([card], className="bg-light vh-100")
        app.run(host=host, port=port, debug=debug)