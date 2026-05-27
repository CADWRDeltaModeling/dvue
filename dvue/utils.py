# use logging
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configure logger to output to standard output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# from stackoverflow.com https://stackoverflow.com/questions/6086976/how-to-get-a-complete-exception-stack-trace-in-python
def full_stack():
    import traceback, sys

    exc = sys.exc_info()[0]
    stack = traceback.extract_stack()[:-1]  # last one would be full_stack()
    if exc is not None:  # i.e. an exception is present
        del stack[-1]  # remove call of full_stack, the printed exception
        # will contain the caught exception caller instead
    trc = "Traceback (most recent call last):\n"
    stackstr = trc + "".join(traceback.format_list(stack))
    if exc is not None:
        stackstr += "  " + traceback.format_exc().lstrip(trc)
    return stackstr


import os
from collections import defaultdict


def get_unique_short_names(paths):
    # Normalize paths and split into parts
    try:
        path_parts = [os.path.normpath(p).split(os.sep) for p in paths]
    except Exception as e:
        logger.error(f"Error normalizing paths: {e}")
        return ""

    # Start with just the basename
    name_map = defaultdict(list)
    for i, parts in enumerate(path_parts):
        base = parts[-1]
        name_map[base].append((i, parts))

    result = [None] * len(paths)

    for base, items in name_map.items():
        if len(items) == 1:
            # No conflict
            i, _ = items[0]
            result[i] = base
        else:
            # Resolve conflict by prepending dirs
            max_depth = max(len(p) for _, p in items)
            for depth in range(2, max_depth + 1):
                temp_names = {}
                conflict = False
                for i, parts in items:
                    short = os.path.join(*parts[-depth:])
                    if short in temp_names:
                        conflict = True
                        break
                    temp_names[short] = i
                if not conflict:
                    # All names are unique with current depth
                    for short, i in temp_names.items():
                        result[i] = short
                    break
            else:
                # Fallback to full path if nothing else works
                for i, parts in items:
                    result[i] = os.path.join(*parts)

    return result


def interpret_file_relative_to(base_dir, fpath):
    full_path = base_dir / fpath
    logger.debug(f"full_path: {full_path}")
    if not full_path.exists():
        logger.warning(f"File {full_path} does not exist. Using {fpath} instead.")
        full_path = fpath
    logger.debug(f"full_path: {full_path}")
    return full_path


def load_geo_dataframe(
    path,
    lat_col="lat",
    lon_col="lon",
    id_col=None,
    crs=None,
):
    """Load geographic station data from a CSV, GeoJSON, shapefile, or GeoPackage.

    Parameters
    ----------
    path : str or Path
        File to load.  Supported formats:

        * ``.geojson`` / ``.json`` / ``.shp`` / ``.gpkg`` — passed directly to
          :func:`geopandas.read_file`.
        * ``.csv`` — coordinate columns are auto-detected in priority order:

          1. *lat_col* / *lon_col* (default ``"lat"`` / ``"lon"``): WGS84
          2. ``"utm_easting"`` / ``"utm_northing"``: EPSG:26910 (NAD83 UTM 10N)
          3. ``"easting"`` / ``"northing"``: EPSG:26910

    lat_col : str
        CSV column name for latitude (WGS84).  Default ``"lat"``.
    lon_col : str
        CSV column name for longitude (WGS84).  Default ``"lon"``.
    id_col : str, optional
        Not used for loading; included for API symmetry with callers that also
        declare the join column alongside the path.
    crs : cartopy CRS or pyproj CRS string, optional
        Override the auto-detected CRS.  Passed to
        :meth:`geopandas.GeoDataFrame.to_crs` for vector files, or used as
        the declared CRS for CSV points.

    Returns
    -------
    geopandas.GeoDataFrame

    Raises
    ------
    ValueError
        If the file extension is not supported, or a CSV has no recognisable
        coordinate columns.
    """
    import geopandas as gpd
    import pandas as pd

    path = str(path)
    ext = os.path.splitext(path)[1].lower()

    if ext in (".geojson", ".json", ".shp", ".gpkg"):
        gdf = gpd.read_file(path)
        if crs is not None:
            gdf = gdf.to_crs(crs)
        return gdf

    if ext == ".csv":
        df = pd.read_csv(path)
        if lat_col in df.columns and lon_col in df.columns:
            geometry = gpd.points_from_xy(df[lon_col], df[lat_col])
            file_crs = crs or "EPSG:4326"
        elif "utm_easting" in df.columns and "utm_northing" in df.columns:
            geometry = gpd.points_from_xy(df["utm_easting"], df["utm_northing"])
            file_crs = crs or "EPSG:26910"
        elif "easting" in df.columns and "northing" in df.columns:
            geometry = gpd.points_from_xy(df["easting"], df["northing"])
            file_crs = crs or "EPSG:26910"
        else:
            raise ValueError(
                f"CSV {path!r} has no recognisable coordinate columns. "
                f"Expected {lat_col!r}/{lon_col!r}, 'utm_easting'/'utm_northing', "
                "or 'easting'/'northing'."
            )
        return gpd.GeoDataFrame(df, geometry=geometry, crs=file_crs)

    raise ValueError(
        f"Unsupported file type for geo loading: {ext!r}. "
        "Supported: .csv, .geojson, .json, .shp, .gpkg"
    )
