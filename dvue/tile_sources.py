"""CARTO basemap tile source configuration.

CARTO's raster basemap tiles (light_all/dark_all/voyager) now require an API
key; requests without one render an "API KEY REQUIRED" watermark over the
map. See https://carto.com/basemaps/apikey to obtain/manage a key.

The key is never hard-coded/committed here. It must be provided via the
``CARTO_API_KEY`` environment variable — this works uniformly for local dev
and server deployments (no reliance on a home directory or a repo-local
file). If it is not set, tiles render without a key (CARTO's watermark
applies) and a one-time warning is logged.
"""

import logging
import os

logger = logging.getLogger(__name__)

_CONFIG_ENV_VAR = "CARTO_API_KEY"

_warned_missing_key = False

CARTO_ATTRIBUTION = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, '
    '&copy; <a href="https://carto.com/attributions">CARTO</a>'
)

# Maps a friendly style name to its path segment under basemaps.cartocdn.com
_CARTO_STYLE_PATHS = {
    "light_all": "light_all",
    "dark_all": "dark_all",
    "voyager": "rastertiles/voyager",
}


def get_carto_api_key():
    """Return the CARTO Basemaps API key from the ``CARTO_API_KEY`` env var, or None.

    Get a free key at https://carto.com/basemaps/apikey
    """
    key = os.environ.get(_CONFIG_ENV_VAR)
    if not key:
        global _warned_missing_key
        if not _warned_missing_key:
            logger.warning(
                "No CARTO API key configured (set the %s environment variable). "
                "Basemap tiles will show CARTO's 'API key required' watermark. "
                "Get a free key at https://carto.com/basemaps/apikey",
                _CONFIG_ENV_VAR,
            )
            _warned_missing_key = True
    return key


def get_carto_tile_url(style="light_all"):
    """Build a CARTO raster tile URL template for the given style, with the API key applied."""
    path = _CARTO_STYLE_PATHS.get(style, style)
    url = f"https://basemaps.cartocdn.com/{path}/{{Z}}/{{X}}/{{Y}}.png"
    key = get_carto_api_key()
    if key:
        url += f"?key={key}"
    return url


def get_carto_tile_source(style="light_all", name="CartoLight"):
    """Build a geoviews WMTS tile source for a CARTO basemap, with the API key applied."""
    import geoviews as gv

    return gv.WMTS(get_carto_tile_url(style), name=name)

