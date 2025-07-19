# fileio.py
import os
import json
# -- CONFIG --
DATALAKE_PREFIX = r"C:\Users\HP\uber_project\Data"

class DataLakeIO:

    def __init__(self,process: str,sourceobject: str = None,state: str = None):
        self.process = process
        self.sourceobject = sourceobject
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
        p = self.process.lower()

        # Raw Uber data: CSV on load, Delta when writing current
        if p in ("load", "read", "write"):
            if self.sourceobject is None:
                raise ValueError("`sourceobject` is required for raw load/read/write")
            so = self.sourceobject.lower()
            if so == "uberfares":
                return "delta" if self.state == "current" else "csv"
            if so == 'features':
                return "delta" if self.state == 'current' else "parquet"

            if so.endswith(".geojson"):
                return "geojson"

        # Enrichment steps: always delta
        if p in ("enrichweather", "enrichdistance", "enrich", "readraw", "readenrich","enrichgeospatial"):
            return "delta"

        raise ValueError(f"No filetype rule for process '{self.process}'")

    # -- PATH BUILDER --
    def filepath(self) -> str:
        """
        Build a platform‑safe path for the given process.

        :param process: see `filetype` docstring
        :param sourceobject: e.g. 'uberfares'
        :param state: e.g. 'current'
        """
        p  = self.process.lower()
        base = DATALAKE_PREFIX.rstrip(os.sep)

        # 1) Raw load
        if p == "load":
            if self.sourceobject and self.sourceobject.lower() == "uberfares":
                return os.path.join(base, "UberFaresData", "uber.csv")
            elif self.sourceobject.endswith(".geojson"):
                so = self.sourceobject.lower()
                parts = [base,"Input","Borough","NewYork",so]
                return os.path.join(*parts)
            else:
                raise ValueError(f"No load path for '{self.sourceobject}'")

        # 2) Raw read/write
        if p in ("read", "write"):
            so = self.sourceobject.lower()
            if so.endswith(".geojson") and p == 'read':
                so = 'fixed_'+so
                parts = [base, "Input", "Borough", "NewYork", so]
                return os.path.join(*parts)
            elif so == 'features':
                parts = [base,"Raw","boroughs","newyork",so]
            else:
                parts = [base, "Raw", so]

            if self.state:
                parts.append(self.state)
            ext = self.filetype()
            filename = f"{so}.{ext}"
            parts.append(filename)
            return os.path.join(*parts)

        # 3) Enrichment
        if p == "enrichweather":
            sub = ["Enrich", "Enriched_Weather_uberData"]
        elif p == "enrichdistance":
            sub = ["Enrich", "Enriched_Distance_uberData"]
        elif p == "enrich":
            sub = ["Enrich", "Enriched"]
        elif p == "readraw":
            so=self.sourceobject.lower()
            if so == 'features':
                sub=["Raw","boroughs","newyork"]
            else:
                sub = ["Raw"]
        elif p == "readenrich":
            sub = ["Enrich", "Enriched"]
        elif p == "enrichgeospatial":
            sub = ["Enrich","Enriched","spatial","newyork"]
        else:
            raise ValueError(f"Unknown process '{self.process}'")

        so = self.sourceobject.lower() if self.sourceobject else ""
        parts = [base] + sub
        if so:
            parts.append(so)
        if self.state:
            parts.append(self.state)
        ext = self.filetype()
        filename = f"{so}.{ext}"
        parts.append(filename)
        return os.path.join(*parts)


class GeoJsonIO:
    def __init__(self,input_filename: str,path: str, validator_func=None):
        self.input_filename = input_filename
        self.validator_func = validator_func
        self.path = path

    def load(self):

        with open(self.path, 'r') as file:
            geojson_input = json.load(file)

        return geojson_input

    def validate_and_fix_geojson(self,validator_func=None):
        """
        Loads, validates, and writes back a fixed GeoJSON file.

        :param input_filename: name of the original GeoJSON file
        :param output_filename: name of the fixed output GeoJSON file
        :param validator_func: a callable that accepts GeoJSON dict and returns a fixed version
        """
        self.validator_func = validator_func
        if self.validator_func is None:
            raise ValueError("A `validator_func` must be provided to validate and fix the GeoJSON.")

        geojson_input = self.load()
        fixed_geojson = self.validator_func(geojson_input)
        output_filename = "fixed_"+self.input_filename
        datalakefileio = DataLakeIO(
            process='write',
            sourceobject=output_filename
        )
        fixed_geojson_path = datalakefileio.filepath()
        with open(fixed_geojson_path, 'w') as file:
            json.dump(fixed_geojson, file, indent=4)

        print("GeoJSON validation and fixing completed successfully.")

        return output_filename