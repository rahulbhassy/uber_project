import os
import json

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

    def __init__(self, process: str, sourceobject: str = None, state: str = None):
        # Cache lowercased values to avoid repeated operations
        self.process = process
        self._process_lower = process.lower()
        self.sourceobject = sourceobject
        self._sourceobject_lower = sourceobject.lower() if sourceobject else None
        self.state = state

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

            if self._sourceobject_lower == "uberfares":
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
            if self._sourceobject_lower == "uberfares":
                return os.path.join(DATALAKE_PREFIX.rstrip(os.sep), "UberFaresData", "uber.csv")
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
                parts = ["Input", "Borough", "NewYork"]
                filename = f"{so}.{ext}"
                return self._build_path_parts(parts, filename)

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