# fileio.py
import os
import json
# -- CONFIG --
DATALAKE_PREFIX = r"C:\Users\HP\uber_project\Data"

# -- FORMAT RESOLVER --
def filetype(process: str, sourceobject: str = None, state: str = None) -> str:
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
    p = process.lower()

    # Raw Uber data: CSV on load, Delta when writing current
    if p in ("load", "read", "write"):
        if sourceobject is None:
            raise ValueError("`sourceobject` is required for raw load/read/write")
        so = sourceobject.lower()
        if so == "uberfares":
            return "delta" if state == "current" else "csv"

    # Enrichment steps: always delta
    if p in ("enrichweather", "enrichdistance", "enrich", "readraw", "readenrich"):
        return "delta"

    raise ValueError(f"No filetype rule for process '{process}'")

# -- PATH BUILDER --
def filepath(process: str, sourceobject: str = None, state: str = None) -> str:
    """
    Build a platform‑safe path for the given process.

    :param process: see `filetype` docstring
    :param sourceobject: e.g. 'uberfares'
    :param state: e.g. 'current'
    """
    p  = process.lower()
    base = DATALAKE_PREFIX.rstrip(os.sep)

    # 1) Raw load
    if p == "load":
        if sourceobject and sourceobject.lower() == "uberfares":
            return os.path.join(base, "UberFaresData", "uber.csv")
        else:
            raise ValueError(f"No load path for '{sourceobject}'")

    # 2) Raw read/write
    if p in ("read", "write"):
        so = sourceobject.lower()
        parts = [base, "Raw", so]
        if state:
            parts.append(state)
        ext = filetype(p, sourceobject, state)
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
        sub = ["Raw"]
    elif p == "readenrich":
        sub = ["Enrich", "Enriched"]
    else:
        raise ValueError(f"Unknown process '{process}'")

    so = sourceobject.lower() if sourceobject else ""
    parts = [base] + sub
    if so:
        parts.append(so)
    if state:
        parts.append(state)
    ext = filetype(p)
    filename = f"{so}.{ext}"
    parts.append(filename)
    return os.path.join(*parts)


def validate_and_fix_geojson(input_filename: str, validator_func=None):
    """
    Loads, validates, and writes back a fixed GeoJSON file.

    :param input_filename: name of the original GeoJSON file
    :param output_filename: name of the fixed output GeoJSON file
    :param validator_func: a callable that accepts GeoJSON dict and returns a fixed version
    """
    if validator_func is None:
        raise ValueError("A `validator_func` must be provided to validate and fix the GeoJSON.")
    prefix = DATALAKE_PREFIX + 'Input/'
    geojson_path = os.path.join(prefix, "Borough", input_filename)
    with open(geojson_path, 'r') as file:
        geojson_input = json.load(file)

    fixed_geojson = validator_func(geojson_input)
    output_filename = "fixed_"+input_filename
    fixed_geojson_path = os.path.join(DATALAKE_PREFIX, "Borough", output_filename)

    with open(fixed_geojson_path, 'w') as file:
        json.dump(fixed_geojson, file, indent=4)

    print("GeoJSON validation and fixing completed successfully.")