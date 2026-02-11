// Frontend API wrapper for WeatherApp.
(function () {
  const BASE = (window.CONFIG && window.CONFIG.API_BASE) ? window.CONFIG.API_BASE.replace(/\/+$/, "") : "";

  /** Build a relative URL with query parameters. */
  function withParams(url, params) {
    const u = new URL(url, window.location.origin);
    Object.entries(params || {}).forEach(([k, v]) => {
      if (v === undefined || v === null || v === "") return;
      u.searchParams.set(k, v);
    });
    return u.toString().replace(window.location.origin, "").replace(/^\/+/, "");
  }

  /** Fetch JSON from the backend with timeout and error handling. */
  async function fetchJson(path, { params, timeoutMs, method, body } = {}) {
    const t = timeoutMs ?? 15000;
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), t);

    const p = params ? `${path}?${new URLSearchParams(params).toString()}` : path;
    const url = `${BASE}${p.startsWith("/") ? "" : "/"}${p}`;

    try {
      const res = await fetch(url, {
        method: method || "GET",
        headers: { "Accept": "application/json", "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const text = await res.text();
      let data;
      try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }

      if (!res.ok) {
        const err = new Error(`HTTP ${res.status}`);
        err.status = res.status;
        err.data = data;
        throw err;
      }
      return data;
    } finally {
      clearTimeout(id);
    }
  }

  /** Try primary path, then fall back for older ML service endpoints. */
  async function fetchJsonWithFallback(primaryPath, fallbackPath, options) {
    try {
      return await fetchJson(primaryPath, options);
    } catch (err) {
      const status = err && err.status;
      if (status === 404 || status === 405 || status === 501 || status === 502 || status === 503) {
        return await fetchJson(fallbackPath, options);
      }
      throw err;
    }
  }

  /** Create a bbox string from a MapLibre map view. */
  function bboxFromMap(map) {
    const b = map.getBounds();
    // minLon,minLat,maxLon,maxLat
    return `${b.getWest()},${b.getSouth()},${b.getEast()},${b.getNorth()}`;
  }

  window.API = {
    base: BASE,

    health: () => fetchJson("/health"),
    metricsSummary: () => fetchJson("/api/metrics/summary", { timeoutMs: 45000 }),
    externalMetrics: () => fetchJson("/api/metrics/external"),

    gridpointsGeojson: (bbox) => fetchJson("/api/gridpoints", { params: bbox ? { bbox } : {} }),
    alertsGeojson: (bbox) => fetchJson("/api/alerts", { params: bbox ? { bbox } : {} }),

    hourlyForecast: (gridId, limit, hours) =>
      fetchJson("/api/forecast/hourly", { params: { gridId, limit: limit ?? window.CONFIG.FORECAST_LIMIT, hours } }),

    dailyForecast: async (gridId, days) => {
      try {
        return await fetchJson("/api/forecast/daily", { params: { gridId, days } });
      } catch (err) {
        if (err && err.status === 404) return [];
        throw err;
      }
    },

    pointLive: (lat, lon) =>
      fetchJson("/api/forecast/hourly/point", { params: { lat, lon } }),

    pointHourlyList: (lat, lon, limit, refresh) =>
      fetchJson("/api/forecast/hourly/point", { params: { lat, lon, limit, mode: "list", refresh: refresh ? 1 : undefined } }),

    historyGridpoint: (gridId, days) =>
      fetchJson("/api/history/gridpoint", { params: { gridId, days } }),

    historyDaily: (stationId, start, end) =>
      fetchJson("/api/history/daily", { params: { stationId, start, end } }),

    mlPredictionsLatest: (gridId, limit) =>
      fetchJson("/api/ml/predictions/latest", { params: { gridId, limit: limit ?? window.CONFIG.ML_LIMIT } }),

    mlWeatherLatest: (sourceType, sourceId, lat, lon) =>
      fetchJsonWithFallback("/api/ml/weather/latest", "/ml/weather/latest", { params: { sourceType, sourceId, lat, lon } }),

    mlWeatherForecast: (sourceType, sourceId, lat, lon, days) =>
      fetchJsonWithFallback("/api/ml/weather/forecast", "/ml/weather/forecast", { params: { sourceType, sourceId, lat, lon, days } }),

    layerTemperature: (hourOffset, bbox) => fetchJson("/layers/temperature", { params: { hourOffset, bbox } }),
    layerPrecip: (range) => fetchJson("/layers/precipitation", { params: { range } }),

    pointSummary: (lat, lon, days, limit) => fetchJson("/api/point/summary", { params: { lat, lon, days, limit } }),

    stationsNear: (lat, lon, limit) => fetchJson("/api/stations/near", { params: { lat, lon, limit } }),
    stationsAll: (bbox, limit, withData) => fetchJson("/api/stations/all", { params: { bbox, limit, withData } }),

    trackedPointsList: () => fetchJson("/api/tracked-points"),
    trackedPointsAdd: (name, lat, lon) => fetchJson("/api/tracked-points", { params: { name, lat, lon }, method: "POST" }),
    trackedPointsRemove: (id) => fetchJson("/api/tracked-points", { params: { id }, method: "DELETE" }),
    trackedPointsRefresh: () => fetchJson("/api/tracked-points/refresh", { method: "POST" }),

    mlPredictStore: (payload) => fetchJson("/ml/predict/store", { method: "POST", body: payload }),
    mlPredict: (payload) => fetchJson("/ml/predict", { method: "POST", body: payload }),
    mlPredictTracked: (payload) => fetchJson("/ml/predict/tracked", { method: "POST", body: payload }),
    mlPredictStations: (payload) => fetchJson("/ml/predict/stations", { method: "POST", body: payload }),

    bboxFromMap
  };
})();
