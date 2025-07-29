import os
import json
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession

# -- CONFIG --
DATALAKE_PREFIX = r"C:\Users\HP\uber_project\Data"


class DataLakeIO:
    # Class-level constants for better performance
    _ENRICHMENT_PROCESSES = {
        "enrichweather": ["Enrich", "Enriched_Weather_uberData"],
        "enrichdistance": ["Enrich", "Enriched_Distance_uberData"],
        "enrich": ["Enrich", "Enriched"],
        "readraw": ["Raw"],  # Special case handled in filepath()
        "readenrich": ["Enrich", "Enriched"],
        "enrichgeospatial": ["Enrich", "Enriched", "spatial", "newyork"]
    }

    _DELTA_PROCESSES = frozenset([
        "enrichweather", "enrichdistance", "enrich", "readraw", "readenrich", "enrichgeospatial"
    ])

    _RAW_PROCESSES = frozenset(["load", "read", "write"])
    _SOURCE_TABLES_CSV = frozenset(["uberfares","tripdetails","driverdetails","customerdetails","vehicledetails"])

    def __init__(self, process: str, loadtype: str = None,sourceobject: str = None, state: str = None):
        # Cache lowercased values to avoid repeated operations
        self.process = process
        self._process_lower = process.lower()
        self.sourceobject = sourceobject
        self._sourceobject_lower = sourceobject.lower() if sourceobject else None
        self.state = state
        self.loadtype = loadtype

    # -- FORMAT RESOLVER --
    def filetype(self) -> str:
        """
        Determine the file format for a given process/source.

        :param process: one of
               - 'load', 'read', 'write'           (for raw Uber data)
               - 'enrichweather', 'enrichdistance' (weather/distance enrich steps)
               - 'enrich'                         (generic enrich)
               - 'readraw', 'readenrich'          (reading back)
        :param sourceobject: e.g. 'uberfares' (only used for raw load/read/write)
        :param state: e.g. 'current'         (only used for raw write)
        """
        # Raw Uber data: CSV on load, Delta when writing current
        if self._process_lower in self._RAW_PROCESSES:
            if self._sourceobject_lower is None:
                raise ValueError("`sourceobject` is required for raw load/read/write")

            if self._sourceobject_lower in self._SOURCE_TABLES_CSV:
                return "delta" if self.state == "current" else "csv"
            elif self._sourceobject_lower == "features":
                return "delta" if self.state == "current" else "parquet"
            elif self._sourceobject_lower.endswith(".geojson"):
                return "geojson"

        # Enrichment steps: always delta
        if self._process_lower in self._DELTA_PROCESSES:
            return "delta"

        raise ValueError(f"No filetype rule for process '{self.process}'")

    def _build_path_parts(self, base_parts, filename):
        """Helper method to build path parts and join them."""
        parts = [DATALAKE_PREFIX.rstrip(os.sep)] + base_parts
        if self.state:
            parts.append(self.state)
        parts.append(filename)
        return os.path.join(*parts)

    # -- PATH BUILDER --
    def filepath(self) -> str:
        """
        Build a platform‑safe path for the given process.

        :param process: see `filetype` docstring
        :param sourceobject: e.g. 'uberfares'
        :param state: e.g. 'current'
        """
        # Cache filetype to avoid multiple calls
        ext = self.filetype()

        # 1) Raw load
        if self._process_lower == "load":
            if self._sourceobject_lower in self._SOURCE_TABLES_CSV:
                today_folder = datetime.now().strftime('%Y-%m-%d')
                if self.loadtype == 'delta':
                    return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), "DataSource",today_folder, f"{self._sourceobject_lower}.csv")
                elif self.loadtype == 'full':
                    return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), "DataSource","*",f"{self._sourceobject_lower}.csv")
            elif self._sourceobject_lower and self._sourceobject_lower.endswith(".geojson"):
                parts = ["Input", "Borough", "NewYork", self._sourceobject_lower]
                return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), *parts)
            else:
                raise ValueError(f"No load path for '{self.sourceobject}'")

        # 2) Raw read/write
        if self._process_lower in ("read", "write"):
            # Handle geojson files
            if self._sourceobject_lower.endswith(".geojson"):
                so = f"fixed_{self._sourceobject_lower}" if self._process_lower == "read" else self._sourceobject_lower
                parts = ["Input", "Borough", "NewYork",so]
                return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), *parts)

            # Handle features
            if self._sourceobject_lower == "features":
                parts = ["Raw", "boroughs", "newyork", self._sourceobject_lower]
            else:
                parts = ["Raw", self._sourceobject_lower]

            filename = f"{self._sourceobject_lower}.{ext}"
            return self._build_path_parts(parts, filename)

        # 3) Enrichment
        if self._process_lower in self._ENRICHMENT_PROCESSES:
            sub = self._ENRICHMENT_PROCESSES[self._process_lower].copy()

            # Special handling for readraw with features
            if self._process_lower == "readraw" and self._sourceobject_lower == "features":
                sub = ["Raw", "boroughs", "newyork"]

            so = self._sourceobject_lower or ""
            parts = sub + ([so] if so else [])
            filename = f"{so}.{ext}"
            return self._build_path_parts(parts, filename)

        raise ValueError(f"Unknown process '{self.process}'")

    def deltafilepath(self,date:str = None):
        intermediateio = IntermediateIO(
            fullpath=self.filepath(),
            date=date
        )
        return intermediateio.get_deltapath()


class IntermediateIO:
    _TABLES = frozenset([
        "uberfares", "tripdetails", "driverdetails",
        "customerdetails", "vehicledetails", "uber","features"
    ])

    def __init__(self, fullpath: str, date: str = None):
        self.fullpath = Path(fullpath)
        self.date = date
        # derive sourceobject once
        self.sourceobject = self._derive_sourceobject()

    def _derive_sourceobject(self) -> str:
        """
        Peel off the DATALAKE_PREFIX and pick the first TABLE name we hit.
        """
        dlprefix = Path(DATALAKE_PREFIX)
        rel = self.fullpath.relative_to(dlprefix)
        for part in rel.parts:
            if part in self._TABLES:
                return part
        raise ValueError(f"No known table found in {self.fullpath}")

    def _get_intermediate_path(self) -> Path:
        """
        Builds <DATALAKE_PREFIX>/.../<sourceobject> (with trailing slash).
        """
        dlprefix = Path(DATALAKE_PREFIX)
        rel = self.fullpath.relative_to(dlprefix)
        ipath = dlprefix
        for part in rel.parts:
            ipath = ipath / part
            if part == self.sourceobject:
                break
        return ipath  # Note: no trailing slash here; Path handles it

    def get_deltapath(self) -> str:
        """
        Returns a string path:
          <intermediatepath>/delta/YYYY-MM-DD/<sourceobject>.parquet
        """
        ipath        = self._get_intermediate_path()
        today_folder = datetime.now().strftime('%Y-%m-%d')
        if self.date:
            today_folder = self.date
        delta_path   = ipath / "delta" / today_folder / f"{self.sourceobject}.parquet"
        return str(delta_path)


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
        datalake_io = DataLakeIO(process='write', sourceobject=output_filename)
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


# Shared/FileIO.py




class SparkTableViewer:
    """
    Dash-based Spark DataFrame viewer.
    If table name is 'spatial', handles geometry serialization.
    """
    from dash import Dash, dash_table, html
    import dash_bootstrap_components as dbc
    from dash_bootstrap_components import Card, CardBody
    from pyspark.sql import DataFrame
    from shapely.geometry import Point, Polygon, LineString

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
