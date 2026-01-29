// Update this to your backend (prefer HTTPS to avoid mixed-content issues)
window.CONFIG = {
  API_BASE: "https://api.ketterling.space",

  // A simple raster basemap style (no tokens)
  MAP_STYLE: {
    "version": 8,
    "sources": {
      "osm": {
        "type": "raster",
        "tiles": [
          "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
          "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
          "https://c.tile.openstreetmap.org/{z}/{x}/{y}.png"
        ],
        "tileSize": 256,
        "attribution": "© OpenStreetMap contributors"
      }
    },
    "layers": [
      { "id": "osm", "type": "raster", "source": "osm" }
    ]
  },

  // Initial view (LA-ish default)
  MAP_START: {
    center: [-118.4, 34.02],
    zoom: 8
  },

  // Fetch limits
  FORECAST_LIMIT: 96,
  ML_LIMIT: 48,

  // Weather card
  WEATHER_HISTORY_DAYS: 30
};
