(function () {
  const el = (id) => document.getElementById(id);

  const apiBaseLabel = el("apiBaseLabel");
  const apiStatus = el("apiStatus");

  const toggleGridpoints = el("toggleGridpoints");
  const toggleAlerts = el("toggleAlerts");
  const toggleStations = el("toggleStations");
  const toggleStationsAll = el("toggleStationsAll");
  const toggleStationsAllWithData = el("toggleStationsAllWithData");
  const toggleTemp = el("toggleTemp");
  const togglePrecip = el("togglePrecip");

  const tempHourOffset = el("tempHourOffset");
  const tempHourLabel = el("tempHourLabel");
  const precipRange = el("precipRange");

  const btnHealth = el("btnHealth");
  const btnMetrics = el("btnMetrics");
  const btnTempLoad = el("btnTempLoad");
  const btnPrecipLoad = el("btnPrecipLoad");
  const btnHistoryReload = el("btnHistoryReload");

  const selectedKind = el("selectedKind");
  const selectedMeta = el("selectedMeta");
  const selectedNote = el("selectedNote");
  const hourlySummary = el("hourlySummary");
  const hourlyChart = el("hourlyChart");
  const btnHourlyToggle = el("btnHourlyToggle");
  const btnHourlyCopy = el("btnHourlyCopy");
  const btnHourlyRefresh = el("btnHourlyRefresh");
  const hourlyTableWrap = el("hourlyTableWrap");
  const hourlyTbody = el("hourlyTable").querySelector("tbody");

  const historyDays = el("historyDays");
  const historyChart = el("historyChart");
  const historySummary = el("historySummary");
  const btnHistoryToggle = el("btnHistoryToggle");
  const historyTableWrap = el("historyTableWrap");
  const historyTbody = el("historyTable").querySelector("tbody");

  const mlStatus = el("mlStatus");
  const mlTbody = el("mlTable").querySelector("tbody");
  const mlWeatherStatus = el("mlWeatherStatus");
  const mlWeatherTodayDate = el("mlWeatherTodayDate");
  const mlWeatherTomorrowDate = el("mlWeatherTomorrowDate");
  const mlWeatherTodayTemp = el("mlWeatherTodayTemp");
  const mlWeatherTomorrowTemp = el("mlWeatherTomorrowTemp");
  const mlWeatherTodayPrcp = el("mlWeatherTodayPrcp");
  const mlWeatherTomorrowPrcp = el("mlWeatherTomorrowPrcp");
  const mlWeatherTodayDelta = el("mlWeatherTodayDelta");
  const mlWeatherTomorrowDelta = el("mlWeatherTomorrowDelta");
  const mlWeatherTodayModel = el("mlWeatherTodayModel");
  const mlWeatherTomorrowModel = el("mlWeatherTomorrowModel");
  const mlForecastStatus = el("mlForecastStatus");
  const mlForecastChart = el("mlForecastChart");
  const btnMlForecastToggle = el("btnMlForecastToggle");
  const mlForecastTableWrap = el("mlForecastTableWrap");
  const mlForecastTbody = el("mlForecastTable")?.querySelector("tbody");

  const mapHud = el("mapHud");

  const poiName = el("poiName");
  const poiLat = el("poiLat");
  const poiLon = el("poiLon");
  const btnPoiAdd = el("btnPoiAdd");
  const btnPoiRefresh = el("btnPoiRefresh");
  const btnPoiMl = el("btnPoiMl");
  const poiList = el("poiList");

  const weatherMode = el("weatherMode");
  const btnFollowView = el("btnFollowView");
  const weatherLocation = el("weatherLocation");
  const weatherLatLon = el("weatherLatLon");
  const weatherTemp = el("weatherTemp");
  const weatherHumidity = el("weatherHumidity");
  const weatherWind = el("weatherWind");
  const weatherPrecip = el("weatherPrecip");
  const weatherSummary = el("weatherSummary");
  const weatherTmean = el("weatherTmean");
  const weatherPrcpWindow = el("weatherPrcpWindow");
  const weatherStations = el("weatherStations");
  const weatherHistoryStation = el("weatherHistoryStation");
  const weatherHistoryTbody = el("weatherHistoryTable")?.querySelector("tbody");
  const utilStations = el("utilStations");
  const utilStationsWithData = el("utilStationsWithData");
  const utilGridpoints = el("utilGridpoints");
  const utilTrackedPoints = el("utilTrackedPoints");
  const utilApiHealth = el("utilApiHealth");
  let lastNearestStations = [];
  let lastStationsAll = null;
  let selectedStationId = null;

  apiBaseLabel.textContent = window.API.base || "(missing CONFIG.API_BASE)";

  async function refreshUtilityCounts() {
    if (!utilStations) return;
    try {
      const data = await window.API.metricsSummary();
      utilStations.textContent = Number.isFinite(data.noaa_stations) ? data.noaa_stations : "--";
      utilStationsWithData.textContent = Number.isFinite(data.noaa_stations_with_data) ? data.noaa_stations_with_data : "--";
      utilGridpoints.textContent = Number.isFinite(data.gridpoints) ? data.gridpoints : "--";
      utilTrackedPoints.textContent = Number.isFinite(data.tracked_points) ? data.tracked_points : "--";
    } catch (e) {
      utilStations.textContent = "--";
      utilStationsWithData.textContent = "--";
      utilGridpoints.textContent = "--";
      utilTrackedPoints.textContent = "--";
    }
  }

  function statusClass(status) {
    if (!status) return "unknown";
    const s = String(status).toLowerCase();
    if (s === "ok") return "ok";
    if (s === "degraded") return "degraded";
    if (s === "down") return "down";
    if (s === "no-data") return "no-data";
    return "unknown";
  }

  async function refreshExternalApiHealth() {
    if (!utilApiHealth) return;
    try {
      const data = await window.API.externalMetrics();
      const services = Array.isArray(data?.services) ? data.services : [];
      if (!services.length) {
        utilApiHealth.textContent = "No data";
        return;
      }

      utilApiHealth.classList.remove("muted");
      utilApiHealth.textContent = "";

      services.forEach(svc => {
        const row = document.createElement("div");
        row.className = "api-health-row";

        const name = document.createElement("div");
        name.className = "api-health-name";
        name.textContent = svc.service || "Unknown";

        const meta = document.createElement("div");
        meta.className = "api-health-meta";

        const status = document.createElement("span");
        const cls = statusClass(svc.status);
        status.className = `badge badge--status badge--${cls}`;
        status.textContent = svc.status || "unknown";

        const calls = document.createElement("span");
        const callsVal = Number.isFinite(svc.calls_last_hour) ? svc.calls_last_hour : 0;
        calls.textContent = `${callsVal}/hr`;

        const fail = document.createElement("span");
        const pct = Number.isFinite(svc.failure_pct) ? svc.failure_pct : 0;
        fail.textContent = `${pct.toFixed(1)}% fail`;

        meta.append(status, calls, fail);
        row.append(name, meta);
        utilApiHealth.appendChild(row);
      });
    } catch (e) {
      utilApiHealth.classList.add("muted");
      utilApiHealth.textContent = "Unavailable";
    }
  }

  function initPanelCollapsing() {
    document.querySelectorAll(".panel").forEach(panel => {
      const title = panel.querySelector(".panel-title");
      if (!title || panel.dataset.collapsibleReady) return;
      panel.dataset.collapsibleReady = "true";
      const body = document.createElement("div");
      body.className = "panel-body";
      let node = title.nextSibling;
      while (node) {
        const next = node.nextSibling;
        body.appendChild(node);
        node = next;
      }
      panel.appendChild(body);

      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "panel-toggle";
      btn.textContent = "Collapse";
      btn.addEventListener("click", () => {
        const collapsed = panel.classList.toggle("is-collapsed");
        btn.textContent = collapsed ? "Expand" : "Collapse";
      });
      title.appendChild(btn);
    });
  }

  initPanelCollapsing();

  // Temperature slider defaults to "Now"

  // ----------------------------
  // Map init
  // ----------------------------
  const map = new maplibregl.Map({
    container: "map",
    style: window.CONFIG.MAP_STYLE,
    center: window.CONFIG.MAP_START.center,
    zoom: window.CONFIG.MAP_START.zoom
  });

  map.addControl(new maplibregl.NavigationControl(), "top-right");

  const hoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 10 });
  let hoverTimer = null;
  let lastHoverKey = null;

  function scheduleHover(key, fn) {
    if (lastHoverKey === key) return;
    lastHoverKey = key;
    clearTimeout(hoverTimer);
    hoverTimer = setTimeout(fn, 250);
  }

  function clearHover() {
    lastHoverKey = null;
    clearTimeout(hoverTimer);
    hoverPopup.remove();
  }

  function setHud() {
    const c = map.getCenter();
    mapHud.textContent = `Zoom ${map.getZoom().toFixed(2)} | ${c.lat.toFixed(4)}, ${c.lng.toFixed(4)}`;
  }

  // Debounce helpers
  let refreshTimer = null;
  function debounceRefresh(fn, ms) {
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(fn, ms);
  }

  let weatherTimer = null;
  function debounceWeather(fn, ms) {
    clearTimeout(weatherTimer);
    weatherTimer = setTimeout(fn, ms);
  }

  const VIEWPORT_MIN_MS = 5000;
  const VIEWPORT_MIN_DELTA = 0.01; // degrees
  let lastViewportFetchAt = 0;
  let lastViewportBbox = null;

  const WEATHER_MIN_MS = 30000;
  const WEATHER_MIN_KM = 1.0;
  let lastWeatherFetchAt = 0;
  let lastWeatherCenter = null;

  function parseBboxString(bboxStr) {
    if (!bboxStr) return null;
    const parts = bboxStr.split(",").map(Number);
    if (parts.length !== 4 || parts.some(n => Number.isNaN(n))) return null;
    return parts;
  }

  function bboxDelta(a, b) {
    if (!a || !b) return Number.POSITIVE_INFINITY;
    let max = 0;
    for (let i = 0; i < 4; i += 1) {
      const d = Math.abs(a[i] - b[i]);
      if (d > max) max = d;
    }
    return max;
  }

  function haversineKm(lat1, lon1, lat2, lon2) {
    const toRad = (deg) => (deg * Math.PI) / 180;
    const R = 6371;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  // ----------------------------
  // Sources/Layers
  // ----------------------------
  const SRC = {
    gridpoints: "gridpoints",
    alerts: "alerts",
    stations: "stations",
    stationsAll: "stationsAll",
    pois: "pois",
    temp: "tempLayer",
    precip: "precipLayer",
    selection: "selectionOverlay"
  };

  function safeSetData(sourceId, geojson) {
    const src = map.getSource(sourceId);
    if (src && src.setData) src.setData(geojson);
  }

  async function refreshViewportLayers() {
    const bboxStr = window.API.bboxFromMap(map);
    const bbox = parseBboxString(bboxStr);
    const now = Date.now();
    if (lastViewportBbox && (now - lastViewportFetchAt) < VIEWPORT_MIN_MS) {
      if (bboxDelta(lastViewportBbox, bbox) < VIEWPORT_MIN_DELTA) {
        return;
      }
    }
    lastViewportFetchAt = now;
    lastViewportBbox = bbox;

    // Gridpoints + Alerts: bbox-scoped
    try {
      if (toggleGridpoints.checked) {
        const gp = await window.API.gridpointsGeojson(bboxStr);
        safeSetData(SRC.gridpoints, gp);
      }
      if (toggleAlerts.checked) {
        const al = await window.API.alertsGeojson(bboxStr);
        safeSetData(SRC.alerts, al);
      }
      if (toggleStationsAll.checked) {
        await loadStationsAll(bboxStr);
      }
    } catch (e) {
      // keep quiet; user can still use other layers
      console.warn("Viewport refresh failed", e);
    }
  }

  function updateTempHourLabel() {
    if (!tempHourOffset || !tempHourLabel) return;
    const hours = Number(tempHourOffset.value || 0);
    if (hours <= 0) {
      tempHourLabel.textContent = "Now";
      return;
    }
    const target = new Date(Date.now() + hours * 3600000);
    const hh = String(target.getHours()).padStart(2, "0");
    const mm = String(target.getMinutes()).padStart(2, "0");
    tempHourLabel.textContent = `+${hours}h (${hh}:${mm})`;
  }

  async function loadTemperatureLayer() {
    const hours = Number(tempHourOffset?.value || 0);
    const gj = await window.API.layerTemperature(hours);
    safeSetData(SRC.temp, gj);
  }

  async function loadPrecipLayer() {
    const r = precipRange.value || "30d";
    const gj = await window.API.layerPrecip(r);
    safeSetData(SRC.precip, gj);
  }

  async function loadNearestStations(lat, lon, limit = 20) {
    if (!toggleStations.checked) return;
    const gj = await window.API.stationsNear(lat, lon, limit);
    safeSetData(SRC.stations, gj);
  }

  async function loadStationsAll(bboxStr, limit = 5000) {
    if (!toggleStationsAll.checked) return;
    if (!bboxStr) return;
    const withData = toggleStationsAllWithData.checked ? "1" : "0";
    const gj = await window.API.stationsAll(bboxStr, limit, withData);
    lastStationsAll = gj;
    safeSetData(SRC.stationsAll, gj);
  }

  function setLayerVisibility(layerId, visible) {
    if (!map.getLayer(layerId)) return;
    map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
  }

  const STATIONS_ALL_CLUSTER_CUTOFF = 8.15;

  function updateStationsAllZoomVisibility(forceVisible) {
    const visible = typeof forceVisible === "boolean" ? forceVisible : toggleStationsAll.checked;
    if (!visible) {
      setLayerVisibility("stations-all-clusters", false);
      setLayerVisibility("stations-all-cluster-count", false);
      setLayerVisibility("stations-all-unclustered", false);
      return;
    }

    const z = map.getZoom();
    const showClusters = z < STATIONS_ALL_CLUSTER_CUTOFF;
    setLayerVisibility("stations-all-clusters", showClusters);
    setLayerVisibility("stations-all-cluster-count", showClusters);
    setLayerVisibility("stations-all-unclustered", !showClusters);
  }

  let pinned = false;
  let pinMarker = null;

  function setPinnedLocation(lngLat, opts = {}) {
    pinned = true;
    btnFollowView.disabled = false;
    if (!pinMarker) {
      const markerEl = document.createElement("div");
      markerEl.className = "pin-marker";
      pinMarker = new maplibregl.Marker({ element: markerEl });
    }
    pinMarker.setLngLat(lngLat).addTo(map);
    if (poiLat) poiLat.value = lngLat.lat.toFixed(5);
    if (poiLon) poiLon.value = lngLat.lng.toFixed(5);
    const label = opts.label || `Pinned ${lngLat.lat.toFixed(4)}, ${lngLat.lng.toFixed(4)}`;
    const contextType = opts.contextType || "pin";
    updateWeatherFor(lngLat.lat, lngLat.lng, "Pinned", true);
    loadNearestStations(lngLat.lat, lngLat.lng, 30);
    if (!opts.skipDetails) {
      loadDetailsForPoint(lngLat.lat, lngLat.lng, label, opts.stationId, contextType, opts.sourceId);
    }
  }

  function clearPinned() {
    pinned = false;
    btnFollowView.disabled = true;
    if (pinMarker) pinMarker.remove();
    const c = map.getCenter();
    updateWeatherFor(c.lat, c.lng, "View", true);
    loadNearestStations(c.lat, c.lng, 20);
  }

  function focusMapOn(lat, lon, zoomOverride) {
    const nextZoom = Number.isFinite(zoomOverride) ? zoomOverride : Math.max(map.getZoom(), 10);
    map.easeTo({ center: [lon, lat], zoom: nextZoom, duration: 800 });
  }

  btnFollowView.addEventListener("click", () => clearPinned());

  map.on("load", async () => {
    // Sources
    map.addSource(SRC.gridpoints, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource(SRC.alerts, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource(SRC.stations, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource(SRC.stationsAll, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
      cluster: true,
      clusterMaxZoom: 8.15,
      clusterRadius: 45
    });
    map.addSource(SRC.pois, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource(SRC.temp, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource(SRC.precip, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    map.addSource(SRC.selection, { type: "geojson", data: { type: "FeatureCollection", features: [] } });

    // Gridpoints: squares
    map.addLayer({
      id: "gridpoints-circle",
      type: "symbol",
      source: SRC.gridpoints,
      layout: {
        "text-field": "■",
        "text-size": ["interpolate", ["linear"], ["zoom"], 4, 10, 8, 12, 12, 16],
        "text-allow-overlap": true
      },
      paint: {
        "text-color": "rgba(130,180,255,0.9)",
        "text-halo-color": "rgba(0,0,0,0.6)",
        "text-halo-width": 1
      }
    });

    // Alerts: fill + outline
    map.addLayer({
      id: "alerts-fill",
      type: "fill",
      source: SRC.alerts,
      paint: {
        "fill-color": "rgba(255,90,90,0.28)",
        "fill-outline-color": "rgba(255,90,90,0.8)"
      }
    });

    map.addLayer({
      id: "alerts-line",
      type: "line",
      source: SRC.alerts,
      paint: {
        "line-color": "rgba(255,90,90,0.9)",
        "line-width": 2
      }
    });

    // Stations: circles colored by tmean
    map.addLayer({
      id: "stations-circle",
      type: "circle",
      source: SRC.stations,
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 6, 3, 10, 6, 12, 8],
        "circle-stroke-width": 1,
        "circle-stroke-color": "rgba(0,0,0,0.6)",
        "circle-color": [
          "interpolate",
          ["linear"],
          ["coalesce", ["get", "tmean_c"], 15],
          -5, "rgba(80,160,255,0.85)",
          10, "rgba(100,210,220,0.85)",
          20, "rgba(255,200,90,0.85)",
          30, "rgba(255,120,90,0.85)"
        ]
      }
    });

    // Stations (all): clustered to handle many points
    map.addLayer({
      id: "stations-all-clusters",
      type: "circle",
      source: SRC.stationsAll,
      filter: ["has", "point_count"],
      paint: {
        "circle-color": "rgba(140, 160, 190, 0.7)",
        "circle-radius": [
          "step",
          ["get", "point_count"],
          14,
          100, 18,
          500, 24,
          1000, 28
        ],
        "circle-stroke-width": 1,
        "circle-stroke-color": "rgba(0,0,0,0.4)"
      }
    });

    map.addLayer({
      id: "stations-all-cluster-count",
      type: "symbol",
      source: SRC.stationsAll,
      filter: ["has", "point_count"],
      layout: {
        "text-field": "{point_count_abbreviated}",
        "text-size": 12,
        "text-font": ["Open Sans Regular", "Arial Unicode MS Regular"],
        "text-allow-overlap": true
      },
      paint: {
        "text-color": "#0f172a"
      }
    });

    map.addLayer({
      id: "stations-all-unclustered",
      type: "circle",
      source: SRC.stationsAll,
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 6, 3, 10, 6, 12, 8],
        "circle-stroke-width": 1,
        "circle-stroke-color": "rgba(0,0,0,0.6)",
        "circle-color": [
          "interpolate",
          ["linear"],
          ["coalesce", ["get", "tmean_c"], 15],
          -5, "rgba(80,160,255,0.85)",
          10, "rgba(100,210,220,0.85)",
          20, "rgba(255,200,90,0.85)",
          30, "rgba(255,120,90,0.85)"
        ]
      }
    });

    // POIs: persistent tracked points (diamond)
    map.addLayer({
      id: "pois-circle",
      type: "symbol",
      source: SRC.pois,
      layout: {
        "text-field": "◆",
        "text-size": ["interpolate", ["linear"], ["zoom"], 6, 12, 10, 14, 12, 18],
        "text-allow-overlap": true
      },
      paint: {
        "text-color": "rgba(240,210,90,0.95)",
        "text-halo-color": "rgba(0,0,0,0.6)",
        "text-halo-width": 1
      }
    });

    // Selection overlay: line + markers
    map.addLayer({
      id: "selection-line",
      type: "line",
      source: SRC.selection,
      filter: ["==", ["geometry-type"], "LineString"],
      paint: {
        "line-color": "rgba(255,200,90,0.85)",
        "line-width": 2,
        "line-dasharray": [1.5, 1.5]
      }
    });

    map.addLayer({
      id: "selection-points",
      type: "symbol",
      source: SRC.selection,
      filter: ["==", ["geometry-type"], "Point"],
      layout: {
        "text-field": [
          "case",
          ["==", ["get", "kind"], "grid"], "■",
          ["==", ["get", "kind"], "station"], "●",
          "◆"
        ],
        "text-size": 16,
        "text-allow-overlap": true
      },
      paint: {
        "text-color": [
          "case",
          ["==", ["get", "kind"], "grid"], "rgba(130,180,255,0.95)",
          ["==", ["get", "kind"], "station"], "rgba(255,200,90,0.95)",
          "rgba(240,210,90,0.95)"
        ],
        "text-halo-color": "rgba(0,0,0,0.6)",
        "text-halo-width": 1
      }
    });

    // Temperature: heatmap from tmean_c
    map.addLayer({
      id: "temp-heat",
      type: "heatmap",
      source: SRC.temp,
      maxzoom: 11,
      paint: {
        "heatmap-weight": [
          "interpolate", ["linear"], ["coalesce", ["get", "temperature_c"], 0],
          -5, 0,
          10, 0.4,
          25, 0.8,
          40, 1.0
        ],
        "heatmap-intensity": [
          "interpolate", ["linear"], ["zoom"],
          5, 0.8,
          10, 1.4
        ],
        "heatmap-color": [
          "interpolate", ["linear"], ["heatmap-density"],
          0, "rgba(0,0,0,0)",
          0.2, "rgba(80,160,255,0.6)",
          0.45, "rgba(100,210,220,0.75)",
          0.7, "rgba(255,200,90,0.8)",
          1.0, "rgba(255,120,90,0.9)"
        ],
        "heatmap-radius": [
          "interpolate", ["linear"], ["zoom"],
          5, 50,
          8, 30,
          11, 8
        ],
        "heatmap-opacity": 0.85
      }
    });

    // Precip: heatmap from prcp_mm
    map.addLayer({
      id: "precip-heat",
      type: "heatmap",
      source: SRC.precip,
      maxzoom: 11,
      paint: {
        "heatmap-weight": [
          "interpolate", ["linear"], ["coalesce", ["get", "prcp_mm"], 0],
          0, 0,
          10, 0.25,
          50, 0.7,
          150, 1.0
        ],
        "heatmap-intensity": [
          "interpolate", ["linear"], ["zoom"],
          5, 0.8,
          10, 1.4
        ],
        "heatmap-radius": [
          "interpolate", ["linear"], ["zoom"],
          5, 18,
          10, 42
        ],
        "heatmap-opacity": 0.85
      }
    });

    // Initial visibility
    setLayerVisibility("gridpoints-circle", toggleGridpoints.checked);
    setLayerVisibility("alerts-fill", toggleAlerts.checked);
    setLayerVisibility("alerts-line", toggleAlerts.checked);
    setLayerVisibility("stations-circle", toggleStations.checked);
    updateStationsAllZoomVisibility(toggleStationsAll.checked);
    setLayerVisibility("temp-heat", toggleTemp.checked);
    setLayerVisibility("precip-heat", togglePrecip.checked);

    // Load initial viewport data
    updateTempHourLabel();
    await refreshViewportLayers();
    setHud();
    const c = map.getCenter();
    await updateWeatherFor(c.lat, c.lng, "View", true);
    await loadNearestStations(c.lat, c.lng, 20);
    await loadTrackedPoints();
    await refreshUtilityCounts();
    await refreshExternalApiHealth();

    setInterval(() => {
      refreshUtilityCounts();
      refreshExternalApiHealth();
    }, 60000);
  });

  map.on("moveend", () => {
    debounceRefresh(refreshViewportLayers, 500);
    if (!pinned) {
      const c = map.getCenter();
      debounceWeather(() => {
        updateWeatherFor(c.lat, c.lng, "View");
        loadNearestStations(c.lat, c.lng, 20);
      }, 700);
    }
  });
  map.on("zoomend", setHud);
  map.on("zoomend", updateStationsAllZoomVisibility);
  map.on("move", setHud);

  // ----------------------------
  // UI actions
  // ----------------------------
  if (btnHourlyToggle) {
    btnHourlyToggle.addEventListener("click", () => {
      const isOpen = hourlyTableWrap && !hourlyTableWrap.classList.contains("is-collapsed");
      setHourlyTableOpen(!isOpen);
    });
  }

  if (btnHourlyRefresh) {
    btnHourlyRefresh.addEventListener("click", () => {
      refreshHourlyForSelected();
    });
  }

  if (btnHistoryToggle) {
    btnHistoryToggle.addEventListener("click", () => {
      const isOpen = historyTableWrap && !historyTableWrap.classList.contains("is-collapsed");
      setHistoryTableOpen(!isOpen);
    });
  }

  if (btnMlForecastToggle) {
    btnMlForecastToggle.addEventListener("click", () => {
      const isOpen = mlForecastTableWrap && !mlForecastTableWrap.classList.contains("is-collapsed");
      setMlForecastTableOpen(!isOpen);
    });
  }

  if (btnHourlyCopy) {
    btnHourlyCopy.addEventListener("click", async () => {
      if (!lastHourlyRows.length) return;
      const header = ["Start", "Temp C", "Precip %", "Wind m/s", "Summary"];
      const lines = lastHourlyRows.map(r => ([
        shortIso(r.start_time),
        fmtNum(r.temperature_c, 1),
        fmtNum(r.precip_prob, 0),
        fmtNum(r.wind_speed_mps, 1),
        (r.short_forecast || "").replace(/\s+/g, " ").trim()
      ].join("\t")));
      const text = [header.join("\t"), ...lines].join("\n");
      const ok = await copyToClipboard(text);
      flashButton(btnHourlyCopy, ok ? "Copied" : "Copy failed");
    });
  }

  let historyResizeTimer = null;
  window.addEventListener("resize", () => {
    clearTimeout(historyResizeTimer);
    historyResizeTimer = setTimeout(() => {
      if (lastHistoryRows.length) renderHistoryChart(lastHistoryRows);
    }, 150);
  });

  toggleGridpoints.addEventListener("change", async () => {
    setLayerVisibility("gridpoints-circle", toggleGridpoints.checked);
    if (toggleGridpoints.checked) await refreshViewportLayers();
  });

  toggleAlerts.addEventListener("change", async () => {
    setLayerVisibility("alerts-fill", toggleAlerts.checked);
    setLayerVisibility("alerts-line", toggleAlerts.checked);
    if (toggleAlerts.checked) await refreshViewportLayers();
  });

  toggleStations.addEventListener("change", async () => {
    if (toggleStations.checked && toggleStationsAll.checked) {
      toggleStationsAll.checked = false;
      updateStationsAllZoomVisibility(false);
      safeSetData(SRC.stationsAll, { type: "FeatureCollection", features: [] });
    }
    setLayerVisibility("stations-circle", toggleStations.checked);
    if (toggleStations.checked) {
      const c = map.getCenter();
      await loadNearestStations(c.lat, c.lng, 20);
    }
  });

  toggleStationsAll.addEventListener("change", async () => {
    if (toggleStationsAll.checked && toggleStations.checked) {
      toggleStations.checked = false;
      setLayerVisibility("stations-circle", false);
    }
    updateStationsAllZoomVisibility(toggleStationsAll.checked);
    if (toggleStationsAll.checked) {
      const bboxStr = window.API.bboxFromMap(map);
      await loadStationsAll(bboxStr);
    } else {
      safeSetData(SRC.stationsAll, { type: "FeatureCollection", features: [] });
    }
  });

  toggleStationsAllWithData.addEventListener("change", () => {
    if (toggleStationsAll.checked) {
      const bboxStr = window.API.bboxFromMap(map);
      loadStationsAll(bboxStr);
    }
  });

  toggleTemp.addEventListener("change", async () => {
    setLayerVisibility("temp-heat", toggleTemp.checked);
    if (toggleTemp.checked) await loadTemperatureLayer();
  });

  if (tempHourOffset) {
    tempHourOffset.addEventListener("input", () => {
      updateTempHourLabel();
      if (toggleTemp.checked) {
        loadTemperatureLayer();
      }
    });
  }

  togglePrecip.addEventListener("change", async () => {
    setLayerVisibility("precip-heat", togglePrecip.checked);
    if (togglePrecip.checked) await loadPrecipLayer();
  });

  btnTempLoad.addEventListener("click", async () => {
    toggleTemp.checked = true;
    setLayerVisibility("temp-heat", true);
    updateTempHourLabel();
    await loadTemperatureLayer();
  });

  btnPrecipLoad.addEventListener("click", async () => {
    togglePrecip.checked = true;
    setLayerVisibility("precip-heat", true);
    await loadPrecipLayer();
  });

  btnHealth.addEventListener("click", async () => {
    apiStatus.textContent = "Loading /health...";
    try {
      const data = await window.API.health();
      apiStatus.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      apiStatus.textContent = `Error: ${e.message}\n${JSON.stringify(e.data || {}, null, 2)}`;
    }
  });

  btnMetrics.addEventListener("click", async () => {
    apiStatus.textContent = "Loading /api/metrics/summary...";
    try {
      const data = await window.API.metricsSummary();
      apiStatus.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      apiStatus.textContent = `Error: ${e.message}\n${JSON.stringify(e.data || {}, null, 2)}`;
    }
  });

  // ----------------------------
  // Tracked points (gridpoints)
  // ----------------------------
  function poiToGeoJson(list) {
    return {
      type: "FeatureCollection",
      features: (list || []).map(p => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
        properties: { id: p.id, name: p.name }
      }))
    };
  }

  async function loadTrackedPoints() {
    try {
      const list = await window.API.trackedPointsList();
      poiList.innerHTML = list.length ? list.map(p => {
        const label = p.name || `Point ${p.id}`;
        return `<div class="poi-item">
          <div class="poi-meta">${escapeHtml(label)} <span class="muted">(${p.lat.toFixed(4)}, ${p.lon.toFixed(4)})</span></div>
          <div class="poi-actions">
            <button class="btn btn-small" data-action="select" data-poi-id="${p.id}" data-poi-lat="${p.lat}" data-poi-lon="${p.lon}" data-poi-name="${encodeURIComponent(label)}">Select</button>
            <button class="btn btn-small" data-action="remove" data-poi-id="${p.id}">Remove</button>
          </div>
        </div>`;
      }).join("") : "No tracked points yet.";

      safeSetData(SRC.pois, poiToGeoJson(list));
    } catch (e) {
      poiList.textContent = `Error loading points: ${e.message}`;
    }
  }

  btnPoiAdd.addEventListener("click", async () => {
    const name = poiName.value.trim() || "Tracked Point";
    const lat = Number(poiLat.value);
    const lon = Number(poiLon.value);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      poiList.textContent = "Lat/Lon required";
      return;
    }

    try {
      const res = await window.API.trackedPointsAdd(name, lat, lon);
      if (res && res.id) {
        try {
          await window.API.mlPredictStore({
            source_type: "tracked",
            source_id: String(res.id),
            lat,
            lon,
            horizon_hours: 24
          });
        } catch (err) {
          // non-blocking
        }
      }
      await loadTrackedPoints();
    } catch (e) {
      poiList.textContent = `Add failed: ${e.message}`;
    }
  });

  btnPoiRefresh.addEventListener("click", async () => {
    try {
      await window.API.trackedPointsRefresh();
    } catch (e) {
      poiList.textContent = `Refresh failed: ${e.message}`;
    }
  });

  if (btnPoiMl) {
    btnPoiMl.addEventListener("click", async () => {
      try {
        await window.API.mlPredictTracked({});
        poiList.textContent = "ML predictions queued for tracked points.";
      } catch (e) {
        poiList.textContent = `ML run failed: ${e.message}`;
      }
    });
  }

  poiList.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const action = btn.getAttribute("data-action");
    if (action === "remove") {
      const id = Number(btn.getAttribute("data-poi-id"));
      if (!Number.isFinite(id)) return;
      try {
        await window.API.trackedPointsRemove(id);
        await loadTrackedPoints();
      } catch (err) {
        poiList.textContent = `Remove failed: ${err.message}`;
      }
      return;
    }
    if (action === "select") {
      const lat = Number(btn.getAttribute("data-poi-lat"));
      const lon = Number(btn.getAttribute("data-poi-lon"));
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
      const nameRaw = btn.getAttribute("data-poi-name") || "";
      let name = "Tracked Point";
      if (nameRaw) {
        try {
          name = decodeURIComponent(nameRaw);
        } catch (err) {
          name = nameRaw;
        }
      }
      const label = `${name} (${lat.toFixed(4)}, ${lon.toFixed(4)})`;
      focusMapOn(lat, lon);
      setPinnedLocation({ lat, lng: lon }, { label, contextType: "tracked", sourceId: String(btn.getAttribute("data-poi-id")) });
    }
  });

  // ----------------------------
  // Selection + details
  // ----------------------------
  let selectedGridId = null;
  let selectedContext = null;
  const SELECTED_INFO = {
    none: { label: "None", badge: "badge-muted", note: "" },
    grid: { label: "Gridpoint", badge: "badge--grid", note: "Saved location; live forecast from NWS point; history from nearest station." },
    station: { label: "Station", badge: "badge--station", note: "History from station; live forecast from nearby NWS gridpoint." },
    pin: { label: "Pinned", badge: "badge--pin", note: "Pinned location; live forecast from NWS point; history from nearest station." },
    tracked: { label: "Tracked", badge: "badge--tracked", note: "Saved location; live forecast from NWS point; history from nearest station." }
  };
  let lastHourlyRows = [];
  let lastHistoryRows = [];
  let lastMlForecastRows = [];

  function clearTables() {
    hourlyTbody.innerHTML = "";
    historyTbody.innerHTML = "";
    mlTbody.innerHTML = "";
    hourlySummary.textContent = "";
    if (historySummary) historySummary.textContent = "";
    hourlyChart.innerHTML = "";
    mlStatus.textContent = "If ML tables exist, they'll show here.";
    if (mlWeatherStatus) mlWeatherStatus.textContent = "No ML prediction yet.";
    if (mlWeatherTodayDate) mlWeatherTodayDate.textContent = "--";
    if (mlWeatherTomorrowDate) mlWeatherTomorrowDate.textContent = "--";
    if (mlWeatherTodayTemp) mlWeatherTodayTemp.textContent = "--";
    if (mlWeatherTomorrowTemp) mlWeatherTomorrowTemp.textContent = "--";
    if (mlWeatherTodayPrcp) mlWeatherTodayPrcp.textContent = "--";
    if (mlWeatherTomorrowPrcp) mlWeatherTomorrowPrcp.textContent = "--";
    if (mlWeatherTodayDelta) mlWeatherTodayDelta.textContent = "--";
    if (mlWeatherTomorrowDelta) mlWeatherTomorrowDelta.textContent = "--";
    if (mlWeatherTodayModel) mlWeatherTodayModel.textContent = "--";
    if (mlWeatherTomorrowModel) mlWeatherTomorrowModel.textContent = "--";
    if (mlForecastStatus) mlForecastStatus.textContent = "No ML forecast yet.";
    if (mlForecastChart) mlForecastChart.innerHTML = "";
    if (mlForecastTbody) mlForecastTbody.innerHTML = "";
    if (mlForecastTableWrap) mlForecastTableWrap.classList.add("is-collapsed");
    setHourlyData([]);
    setHistoryData([]);
    setMlForecastData([]);
  }

  function setSelectedHeader(type, metaText, noteText) {
    const info = SELECTED_INFO[type] || SELECTED_INFO.none;
    if (selectedKind) {
      selectedKind.textContent = info.label;
      selectedKind.className = `badge ${info.badge || "badge-muted"}`;
    }
    if (selectedMeta) selectedMeta.textContent = metaText || "";
    if (selectedNote) selectedNote.textContent = noteText != null ? noteText : (info.note || "");
  }

  function setHourlyTableOpen(open) {
    if (!hourlyTableWrap) return;
    hourlyTableWrap.classList.toggle("is-collapsed", !open);
    if (btnHourlyToggle) btnHourlyToggle.textContent = open ? "Hide data" : "Show data";
  }

  function setMlForecastTableOpen(open) {
    if (!mlForecastTableWrap) return;
    mlForecastTableWrap.classList.toggle("is-collapsed", !open);
    if (btnMlForecastToggle) btnMlForecastToggle.textContent = open ? "Hide data" : "Show data";
  }

  function setHistoryTableOpen(open) {
    if (!historyTableWrap) return;
    historyTableWrap.classList.toggle("is-collapsed", !open);
    if (btnHistoryToggle) btnHistoryToggle.textContent = open ? "Hide data" : "Show data";
  }

  function setHourlyData(rows) {
    const list = Array.isArray(rows) ? rows.slice(0, 24) : [];
    lastHourlyRows = list;
    if (btnHourlyCopy) btnHourlyCopy.disabled = list.length === 0;
  }

  async function refreshHourlyForSelected() {
    if (!hourlySummary) return;
    let lat = selectedContext && Number.isFinite(selectedContext.lat) ? selectedContext.lat : null;
    let lon = selectedContext && Number.isFinite(selectedContext.lon) ? selectedContext.lon : null;
    if (lat == null || lon == null) {
      const c = map.getCenter();
      lat = c.lat;
      lon = c.lng;
    }
    hourlySummary.textContent = "Refreshing hourly...";
    try {
      const hourlyLive = await window.API.pointHourlyList(lat, lon, 24);
      const list = hourlyLive && hourlyLive.periods ? hourlyLive.periods : [];
      renderHourlyForecast(list);
      setHourlyData(list);
    } catch (e) {
      renderHourlyForecast([], `Hourly error: ${e.message}`);
      setHourlyData([]);
    }
  }

  function setHistoryData(rows) {
    lastHistoryRows = Array.isArray(rows) ? rows : [];
    renderHistoryChart(lastHistoryRows);
    if (historySummary) {
      historySummary.textContent = lastHistoryRows.length ? `rows=${lastHistoryRows.length}` : "No history rows";
    }
  }

  function setMlForecastData(rows, nwsRows) {
    lastMlForecastRows = Array.isArray(rows) ? rows : [];
    renderMlForecastChart(lastMlForecastRows, nwsRows || []);
  }

  function setMlWeatherData(pred, forecastTemp) {
    if (!mlWeatherStatus) return;
    if (!pred) {
      mlWeatherStatus.textContent = "No ML prediction yet.";
      mlWeatherTodayDate.textContent = "--";
      mlWeatherTomorrowDate.textContent = "--";
      mlWeatherTodayTemp.textContent = "--";
      mlWeatherTomorrowTemp.textContent = "--";
      mlWeatherTodayPrcp.textContent = "--";
      mlWeatherTomorrowPrcp.textContent = "--";
      mlWeatherTodayDelta.textContent = "--";
      mlWeatherTomorrowDelta.textContent = "--";
      mlWeatherTodayModel.textContent = "--";
      mlWeatherTomorrowModel.textContent = "--";
      return;
    }
    const asOf = pred.date || pred.as_of_date || "";
    const asOfLabel = asOf ? formatShortDate(asOf) : "--";
    mlWeatherStatus.textContent = asOf ? `For ${asOfLabel}` : "ML prediction";
    mlWeatherTodayDate.textContent = asOfLabel;
    mlWeatherTomorrowDate.textContent = "--";
    mlWeatherTodayTemp.textContent = pred.tmean_c != null ? `${fmtNum(pred.tmean_c, 1)} C` : "--";
    mlWeatherTomorrowTemp.textContent = "--";
    mlWeatherTodayPrcp.textContent = pred.prcp_mm != null ? `${fmtNum(pred.prcp_mm, 1)} mm` : "--";
    mlWeatherTomorrowPrcp.textContent = "--";
    if (Number.isFinite(forecastTemp) && pred.tmean_c != null) {
      const delta = Number(pred.tmean_c) - Number(forecastTemp);
      mlWeatherTodayDelta.textContent = `${fmtNum(delta, 1)} C`;
    } else {
      mlWeatherTodayDelta.textContent = "--";
    }
    mlWeatherTomorrowDelta.textContent = "--";
    const modelLabel = pred.model_detail
      ? `${pred.model_name || "model"} (${pred.model_detail})`
      : (pred.model_name || "knn-distance");
    mlWeatherTodayModel.textContent = modelLabel;
    mlWeatherTomorrowModel.textContent = modelLabel;
  }

  function renderMlForecastChart(rows, nwsRows = []) {
    if (!mlForecastChart) return;
    mlForecastChart.innerHTML = "";
    const list = Array.isArray(rows) ? rows.slice(0, 10) : [];
    if (!list.length) {
      mlForecastChart.innerHTML = "<div class=\"empty\">No forecast data</div>";
      return;
    }

    const tempsMin = list.map(r => r.tmin_c).filter(v => Number.isFinite(v));
    const tempsMax = list.map(r => r.tmax_c).filter(v => Number.isFinite(v));
    const tempsMean = list.map(r => r.tmean_c).filter(v => Number.isFinite(v));
    const nwsMin = (nwsRows || []).map(r => r.tmin_c).filter(v => Number.isFinite(v));
    const nwsMax = (nwsRows || []).map(r => r.tmax_c).filter(v => Number.isFinite(v));
    const minT = Math.min(
      tempsMin.length ? Math.min(...tempsMin) : Math.min(...tempsMean, 0),
      nwsMin.length ? Math.min(...nwsMin) : Infinity
    );
    const maxT = Math.max(
      tempsMax.length ? Math.max(...tempsMax) : Math.max(...tempsMean, 1),
      nwsMax.length ? Math.max(...nwsMax) : -Infinity
    );

    const width = mlForecastChart.clientWidth;
    const height = mlForecastChart.clientHeight;
    if (!width || !height) return;

    const canvas = document.createElement("canvas");
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, Math.floor(width * dpr));
    canvas.height = Math.max(1, Math.floor(height * dpr));
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    mlForecastChart.appendChild(canvas);
    attachHoverGrid(mlForecastChart);

    const ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);

    const pad = { left: 30, right: 8, top: 8, bottom: 20 };
    const innerW = width - pad.left - pad.right;
    const innerH = height - pad.top - pad.bottom;
    const n = list.length;

    const yFor = (t) => pad.top + (1 - (t - minT) / (maxT - minT || 1)) * innerH;

    ctx.strokeStyle = "rgba(255,255,255,0.2)";
    ctx.beginPath();
    ctx.moveTo(pad.left, yFor(minT));
    ctx.lineTo(pad.left + innerW, yFor(minT));
    ctx.moveTo(pad.left, yFor(maxT));
    ctx.lineTo(pad.left + innerW, yFor(maxT));
    ctx.stroke();

    // Min line (blue)
    ctx.strokeStyle = "rgba(120,180,255,0.9)";
    ctx.lineWidth = 2;
    ctx.beginPath();
    list.forEach((r, idx) => {
      if (!Number.isFinite(r.tmin_c)) return;
      const x = pad.left + (idx / Math.max(1, n - 1)) * innerW;
      const y = yFor(r.tmin_c);
      if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // Max line (red)
    ctx.strokeStyle = "rgba(255,90,90,0.9)";
    ctx.beginPath();
    list.forEach((r, idx) => {
      if (!Number.isFinite(r.tmax_c)) return;
      const x = pad.left + (idx / Math.max(1, n - 1)) * innerW;
      const y = yFor(r.tmax_c);
      if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // Mean line (black)
    ctx.strokeStyle = "rgba(0,0,0,0.9)";
    ctx.lineWidth = 2;
    ctx.beginPath();
    list.forEach((r, idx) => {
      if (!Number.isFinite(r.tmean_c)) return;
      const x = pad.left + (idx / Math.max(1, n - 1)) * innerW;
      const y = yFor(r.tmean_c);
      if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // NWS mean line (if available)
    if (nwsRows && nwsRows.length) {
      ctx.strokeStyle = "rgba(160,200,255,0.75)";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      nwsRows.forEach((r, idx) => {
        const mean = (Number.isFinite(r.tmin_c) && Number.isFinite(r.tmax_c))
          ? (r.tmin_c + r.tmax_c) / 2.0
          : null;
        if (mean == null) return;
        const x = pad.left + (idx / Math.max(1, (nwsRows.length - 1))) * innerW;
        const y = yFor(mean);
        if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.setLineDash([]);
    }

    ctx.fillStyle = "rgba(255,255,255,0.7)";
    ctx.font = "10px system-ui";
    ctx.fillText(`${fmtNum(maxT, 1)} C`, 2, pad.top + 8);
    ctx.fillText(`${fmtNum(minT, 1)} C`, 2, pad.top + innerH - 2);

    // Day labels
    ctx.fillStyle = "rgba(255,255,255,0.6)";
    ctx.font = "10px system-ui";
    list.forEach((r, idx) => {
      const date = r.as_of_date
        ? new Date(`${r.as_of_date}T00:00:00Z`) : null;
      const day = date ? new Date(date.getTime() + (r.horizon_hours || 0) * 3600000) : null;
      if (!day) return;
      const label = day.toLocaleDateString([], { weekday: "short" });
      const x = pad.left + (idx / Math.max(1, n - 1)) * innerW;
      ctx.fillText(label, x - 10, pad.top + innerH + 14);
    });
  }

  function getMlAsOfDate() {
    if (Array.isArray(lastHistoryRows) && lastHistoryRows.length) {
      const last = lastHistoryRows[lastHistoryRows.length - 1];
      if (last && last.date) return last.date;
    }
    if (Array.isArray(lastNearestStations) && lastNearestStations.length) {
      const d = lastNearestStations[0]?.latest_date;
      if (d && d !== "null") return d;
    }
    return null;
  }

  async function updateMlWeather(sourceType, sourceId, lat, lon, forecastTemp, gridId, fallbackStationId) {
    if (!mlWeatherStatus) return;
    mlWeatherStatus.textContent = "Updating ML...";
    const asOfDate = getMlAsOfDate();
    try {
      const pred = await window.API.mlPredictStore({
        source_type: sourceType,
        source_id: sourceId,
        lat,
        lon,
        horizon_hours: 24,
        date: asOfDate
      });
      setMlWeatherData(pred, forecastTemp);
    } catch (e) {
      try {
        const pred = await window.API.mlPredict({
          source_type: sourceType,
          source_id: sourceId,
          lat,
          lon,
          horizon_hours: 24,
          date: asOfDate
        });
        setMlWeatherData(pred, forecastTemp);
      } catch (err) {
        try {
          const pred = await window.API.mlWeatherLatest(sourceType, sourceId, lat, lon);
          setMlWeatherData(pred, forecastTemp);
        } catch (finalErr) {
          mlWeatherStatus.textContent = `ML unavailable: ${finalErr.message}`;
        }
      }
    }

    try {
      let forecastSourceType = sourceType;
      let forecastSourceId = sourceId;
      if ((sourceType === "point" || sourceType === "tracked") && gridId) {
        forecastSourceType = "gridpoint";
        forecastSourceId = gridId;
      }

      let forecast = [];
      let forecastErr = null;
      let usedFallback = false;
      try {
        forecast = await window.API.mlWeatherForecast(forecastSourceType, forecastSourceId, lat, lon, 10);
      } catch (err) {
        forecastErr = err;
      }

      if ((!forecast || !forecast.length) && fallbackStationId && forecastSourceType !== "station") {
        try {
          forecast = await window.API.mlWeatherForecast("station", fallbackStationId, lat, lon, 10);
          usedFallback = true;
          forecastErr = null;
        } catch (err) {
          if (!forecastErr) forecastErr = err;
        }
      }

      if (forecastErr && (!forecast || !forecast.length)) {
        throw forecastErr;
      }

      const nwsDaily = gridId ? await window.API.dailyForecast(gridId, 10) : [];
      if (mlForecastStatus) {
        if (forecast && forecast.length) {
          const detail = forecast[0]?.model_detail || forecast[0]?.model_name || "model";
          const asOf = forecast[0]?.as_of_date || "";
          const fallbackNote = usedFallback ? " • station fallback" : "";
          mlForecastStatus.textContent = `as of ${asOf} • days=${forecast.length} • ${detail}${fallbackNote}`.trim();
        } else {
          mlForecastStatus.textContent = "No ML forecast yet.";
        }
      }

      const nwsByDay = new Map();
      (nwsDaily || []).forEach(d => {
        const key = (d.day || "").slice(0, 10);
        if (key) nwsByDay.set(key, d);
      });

      if (mlForecastTbody) {
        mlForecastTbody.innerHTML = (forecast || []).map(r => {
          const date = r.as_of_date
            ? new Date(`${r.as_of_date}T00:00:00Z`) : null;
          const day = date ? new Date(date.getTime() + (r.horizon_hours || 0) * 3600000) : null;
          const label = day ? day.toISOString().slice(0, 10) : "";
          const nws = nwsByDay.get(label) || {};
          return `<tr>
            <td>${label}</td>
            <td>${fmtNum(r.tmin_c, 1)}</td>
            <td>${fmtNum(r.tmax_c, 1)}</td>
            <td>${fmtNum(r.tmean_c, 1)}</td>
            <td>${fmtNum(r.delta_c, 1)}</td>
            <td>${fmtNum(r.prcp_mm, 1)}</td>
            <td>${fmtNum(nws.tmin_c, 1)}</td>
            <td>${fmtNum(nws.tmax_c, 1)}</td>
          </tr>`;
        }).join("");
      }
      setMlForecastData(forecast || [], nwsDaily || []);

      // Update ML Weather today/tomorrow from forecast rows if available
      if (forecast && forecast.length) {
        const byH = new Map((forecast || []).map(r => [Number(r.horizon_hours || 0), r]));
        const todayRow = byH.get(0) || forecast[0];
        const tomorrowRow = byH.get(24) || forecast[1];
        if (todayRow && todayRow.as_of_date) {
          mlWeatherTodayDate.textContent = formatShortDate(todayRow.as_of_date);
          mlWeatherTodayTemp.textContent = todayRow.tmean_c != null ? `${fmtNum(todayRow.tmean_c, 1)} C` : "--";
          mlWeatherTodayPrcp.textContent = todayRow.prcp_mm != null ? `${fmtNum(todayRow.prcp_mm, 1)} mm` : "--";
          mlWeatherTodayDelta.textContent = todayRow.delta_c != null ? `${fmtNum(todayRow.delta_c, 1)} C` : "--";
        }
        if (tomorrowRow && tomorrowRow.as_of_date) {
          const base = new Date(`${tomorrowRow.as_of_date}T00:00:00Z`);
          const day = new Date(base.getTime() + (tomorrowRow.horizon_hours || 0) * 3600000);
          mlWeatherTomorrowDate.textContent = formatShortDate(day.toISOString().slice(0, 10));
          mlWeatherTomorrowTemp.textContent = tomorrowRow.tmean_c != null ? `${fmtNum(tomorrowRow.tmean_c, 1)} C` : "--";
          mlWeatherTomorrowPrcp.textContent = tomorrowRow.prcp_mm != null ? `${fmtNum(tomorrowRow.prcp_mm, 1)} mm` : "--";
          mlWeatherTomorrowDelta.textContent = tomorrowRow.delta_c != null ? `${fmtNum(tomorrowRow.delta_c, 1)} C` : "--";
        }

        const modelLabel = forecast[0]?.model_detail
          ? `${forecast[0]?.model_name || "model"} (${forecast[0]?.model_detail})`
          : (forecast[0]?.model_name || "model");
        if (mlWeatherTodayModel) mlWeatherTodayModel.textContent = modelLabel;
        if (mlWeatherTomorrowModel) mlWeatherTomorrowModel.textContent = modelLabel;
      }
    } catch (e) {
      if (mlForecastStatus) mlForecastStatus.textContent = `ML forecast unavailable: ${e.message}`;
      setMlForecastData([]);
    }
  }

  function renderHistoryChart(rows) {
    if (!historyChart) return;
    historyChart.innerHTML = "";
    const chartRows = (rows || []).filter(r => r && (Number.isFinite(r.tmin_c) || Number.isFinite(r.tmax_c)));
    if (!chartRows.length) {
      historyChart.innerHTML = "<div class=\"empty\">No history data</div>";
      return;
    }

    const clippedRows = chartRows.slice(-365);
    const temps = [];
    clippedRows.forEach(r => {
      if (Number.isFinite(r.tmin_c)) temps.push(r.tmin_c);
      if (Number.isFinite(r.tmax_c)) temps.push(r.tmax_c);
    });
    if (!temps.length) {
      historyChart.innerHTML = "<div class=\"empty\">No temperature data</div>";
      return;
    }

    const width = historyChart.clientWidth;
    const height = historyChart.clientHeight;
    if (!width || !height) return;

    const canvas = document.createElement("canvas");
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, Math.floor(width * dpr));
    canvas.height = Math.max(1, Math.floor(height * dpr));
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    historyChart.appendChild(canvas);
    attachHoverGrid(historyChart);

    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.scale(dpr, dpr);

    const pad = { left: 28, right: 8, top: 8, bottom: 18 };
    const chartW = Math.max(1, width - pad.left - pad.right);
    const chartH = Math.max(1, height - pad.top - pad.bottom);
    const minT = Math.min(...temps);
    const maxT = Math.max(...temps);
    const range = Math.max(1, maxT - minT);
    const denom = Math.max(1, clippedRows.length - 1);

    ctx.strokeStyle = "rgba(90,160,255,0.85)";
    ctx.lineWidth = 1;

    clippedRows.forEach((r, i) => {
      if (!Number.isFinite(r.tmin_c) || !Number.isFinite(r.tmax_c)) return;
      const x = pad.left + (i / denom) * chartW;
      const yMax = pad.top + (1 - (r.tmax_c - minT) / range) * chartH;
      const yMin = pad.top + (1 - (r.tmin_c - minT) / range) * chartH;
      ctx.beginPath();
      ctx.moveTo(x, yMax);
      ctx.lineTo(x, yMin);
      ctx.stroke();
    });

    ctx.fillStyle = "rgba(159,176,194,0.85)";
    ctx.font = "10px system-ui";
    ctx.fillText(`${fmtNum(maxT, 1)} C`, 2, pad.top + 8);
    ctx.fillText(`${fmtNum(minT, 1)} C`, 2, height - 6);

    const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let lastMonth = null;
    let lastLabelX = -Infinity;
    clippedRows.forEach((r, i) => {
      if (!r.date) return;
      const d = new Date(r.date);
      if (Number.isNaN(d.getTime())) return;
      const m = d.getMonth();
      if (m === lastMonth) return;
      lastMonth = m;
      const x = pad.left + (i / denom) * chartW;
      if (x - lastLabelX < 26) return;
      lastLabelX = x;
      ctx.fillText(monthNames[m], x - 8, height - 4);
    });
  }

  async function copyToClipboard(text) {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "true");
    ta.style.position = "fixed";
    ta.style.top = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    let ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (err) {
      ok = false;
    } finally {
      document.body.removeChild(ta);
    }
    return ok;
  }

  function flashButton(btn, label) {
    if (!btn) return;
    const prev = btn.textContent;
    btn.textContent = label;
    setTimeout(() => {
      btn.textContent = prev;
    }, 1200);
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  setSelectedHeader("none", "Click a gridpoint.", "");
  setHourlyTableOpen(false);
  setHistoryTableOpen(false);

  function fmtNum(v, digits = 1) {
    if (v === null || v === undefined) return "";
    if (Number.isFinite(v)) return v.toFixed(digits);
    return String(v);
  }

  function formatShortDate(value) {
    if (!value) return "--";
    const m = String(value).match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) return String(value);
    return `${Number(m[2])}/${Number(m[3])}/${m[1]}`;
  }

  function shortIso(s) {
    if (!s) return "";
    // "2026-01-25T21:02:23Z" -> "01-25 21:02"
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
    return m ? `${m[2]}-${m[3]} ${m[4]}:${m[5]}` : s;
  }

  function renderHourlyForecast(rows, emptyMessage) {
    const list = Array.isArray(rows) ? rows : [];
    const next24 = list.slice(0, 24);
    if (!list.length) {
      hourlySummary.textContent = emptyMessage || "No hourly forecast available";
      hourlyChart.innerHTML = "";
      hourlyTbody.innerHTML = "";
      setHourlyData([]);
      return;
    }

    hourlySummary.textContent = `rows=${next24.length} (next 24h)`;
    renderHourlyChart(next24);
    const now = new Date();
    hourlyTbody.innerHTML = next24.map(r => {
      const start = r.start_time ? new Date(r.start_time) : null;
      const end = r.end_time ? new Date(r.end_time) : null;
      const isNow = start && end ? (now >= start && now < end) : false;
      return `<tr>
        <td class="${isNow ? "now-row" : ""}">${shortIso(r.start_time)}</td>
        <td>${fmtNum(r.temperature_c, 1)}</td>
        <td>${fmtNum(r.precip_prob, 0)}</td>
        <td>${fmtNum(r.wind_speed_mps, 1)}</td>
        <td>${(r.short_forecast || "").slice(0, 42)}</td>
      </tr>`;
    }).join("");
    setHourlyData(next24);
  }

  function historyRange(days) {
    const end = new Date();
    end.setDate(end.getDate() - 1);
    const start = new Date(end);
    start.setDate(start.getDate() - (days - 1));
    const fmt = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    return { start: fmt(start), end: fmt(end) };
  }

  async function fetchHistoryForStation(stationId, days) {
    const { start, end } = historyRange(days);
    return window.API.historyDaily(stationId, start, end);
  }

  async function loadHistoryForStation(stationId, days) {
    const hist = await fetchHistoryForStation(stationId, days);
    historyTbody.innerHTML = hist.slice(-400).map(r => {
      return `<tr>
        <td>${r.date || ""}</td>
        <td>${fmtNum(r.tmax_c, 1)}</td>
        <td>${fmtNum(r.tmin_c, 1)}</td>
        <td>${fmtNum(r.prcp_mm, 1)}</td>
      </tr>`;
    }).join("");
    const chartDays = 365;
    if (days < chartDays) {
      try {
        const chartRange = historyRange(chartDays);
        const chartHist = await window.API.historyDaily(stationId, chartRange.start, chartRange.end);
        setHistoryData(chartHist);
      } catch (e) {
        setHistoryData(hist);
      }
    } else {
      setHistoryData(hist);
    }
    return hist;
  }

  async function loadHistoryFromNearestStations(stations, days) {
    const list = Array.isArray(stations) ? stations : [];
    for (const s of list) {
      const stationId = s.station_id;
      if (!stationId) continue;
      try {
        const hist = await fetchHistoryForStation(stationId, days);
        if (hist && hist.length) {
          historyTbody.innerHTML = hist.slice(-400).map(r => {
            return `<tr>
              <td>${r.date || ""}</td>
              <td>${fmtNum(r.tmax_c, 1)}</td>
              <td>${fmtNum(r.tmin_c, 1)}</td>
              <td>${fmtNum(r.prcp_mm, 1)}</td>
            </tr>`;
          }).join("");
          setHistoryData(hist);
          return { station: s, history: hist };
        }
      } catch (e) {
        // keep searching
      }
    }
    historyTbody.innerHTML = "<tr><td colspan=\"4\">No station history available</td></tr>";
    setHistoryData([]);
    return { station: null, history: [] };
  }

  async function loadDetailsForPoint(lat, lon, label, stationOverride, contextType = "pin", sourceId) {
    selectedGridId = null;
    const resolvedType = SELECTED_INFO[contextType] ? contextType : "pin";
    selectedContext = { type: resolvedType, lat, lon, label, stationId: stationOverride || null, sourceId: sourceId || null };
    clearTables();

    const displayLabel = label || `Pinned ${lat.toFixed(4)}, ${lon.toFixed(4)}`;
    setSelectedHeader(resolvedType, displayLabel);

    try {
      const days = Number(historyDays.value || 365);
      const data = await window.API.pointSummary(lat, lon, days, 25);

        let forecastTemp = null;
        let gridRef = null;
        try {
          const hourlyLive = await window.API.pointHourlyList(lat, lon, 24);
          const list = hourlyLive && hourlyLive.periods ? hourlyLive.periods : [];
          renderHourlyForecast(list);
          if (list && list.length && Number.isFinite(list[0].temperature_c)) {
            forecastTemp = list[0].temperature_c;
          }
          if (hourlyLive && hourlyLive.grid_id) {
            gridRef = { grid_id: hourlyLive.grid_id, lat, lon };
          }
        } catch (err) {
          renderHourlyForecast([], "No live hourly forecast for this location");
        }

      const stationList = Array.isArray(data.nearest_stations) ? data.nearest_stations.slice() : [];
      let stationRef = null;
      if (stationOverride) {
        stationRef = stationList.find(s => s.station_id === stationOverride) || {
          station_id: stationOverride,
          name: label,
          lat,
          lon
        };
        stationList.unshift(stationRef);
      } else if (stationList.length) {
        stationRef = stationList[0];
      }

      const historyResult = await loadHistoryFromNearestStations(stationList, days);
      const historyStation = historyResult.station || stationRef;

      if (historyStation) {
        selectedStationId = historyStation.station_id || null;
          const note = buildSourceNote({ grid: gridRef || data.nearest_gridpoint, station: historyStation, stationName: historyStation.name });
        setSelectedHeader(resolvedType, displayLabel, note);
        if (Number.isFinite(Number(historyStation.lat)) && Number.isFinite(Number(historyStation.lon))) {
          updateSelectionOverlay({ lat, lon }, { lat: Number(historyStation.lat), lon: Number(historyStation.lon) }, gridRef);
        } else {
          updateSelectionOverlay(null, null, gridRef);
        }
      } else {
        selectedStationId = null;
          setSelectedHeader(resolvedType, displayLabel, buildSourceNote({ grid: gridRef || data.nearest_gridpoint }));
        updateSelectionOverlay(null, null, gridRef);
      }

      if (lastNearestStations.length) {
        renderStations(lastNearestStations);
      }

      const mlSourceType = resolvedType === "pin" ? "point" : resolvedType;
      const mlSourceId = resolvedType === "station" ? (historyStation && historyStation.station_id) : (resolvedType === "tracked" ? sourceId : null);
      const gridId = gridRef && gridRef.grid_id ? gridRef.grid_id : null;
      const fallbackStationId = historyStation && historyStation.station_id ? historyStation.station_id : null;
      await updateMlWeather(mlSourceType, mlSourceId, lat, lon, forecastTemp, gridId, fallbackStationId);
    } catch (e) {
      renderHourlyForecast([], `Point error: ${e.message}`);
      historyTbody.innerHTML = `<tr><td colspan="4">History error: ${e.message}</td></tr>`;
      setHistoryData([]);
      setMlWeatherData(null, null);
      updateSelectionOverlay(null, null, null);
    }
  }

  async function loadDetailsForStation(stationId, name, lat, lon) {
    const label = name || stationId;
    await loadDetailsForPoint(lat, lon, label, stationId, "station", stationId);
  }

  function attachHoverGrid(container) {
    if (!container || container.dataset.hoverGridReady) return;
    container.dataset.hoverGridReady = "true";
    const xLine = document.createElement("div");
    const yLine = document.createElement("div");
    xLine.className = "chart-crosshair-x";
    yLine.className = "chart-crosshair-y";
    container.appendChild(xLine);
    container.appendChild(yLine);

    container.addEventListener("mousemove", (e) => {
      const rect = container.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      container.classList.add("chart-hover");
      xLine.style.top = `${Math.max(0, Math.min(rect.height, y))}px`;
      yLine.style.left = `${Math.max(0, Math.min(rect.width, x))}px`;
    });

    container.addEventListener("mouseleave", () => {
      container.classList.remove("chart-hover");
    });
  }

  function renderHourlyChart(rows) {
    if (!rows || rows.length === 0) {
      hourlyChart.innerHTML = "";
      return;
    }
    const temps = rows.map(r => r.temperature_c).filter(v => Number.isFinite(v));
    const minT = temps.length ? Math.min(...temps) : 0;
    const maxT = temps.length ? Math.max(...temps) : 1;
    const midT = (minT + maxT) / 2.0;
    const now = new Date();

    const bars = rows.map(r => {
      const t = Number.isFinite(r.temperature_c) ? r.temperature_c : null;
      const start = r.start_time ? new Date(r.start_time) : null;
      const end = r.end_time ? new Date(r.end_time) : null;
      const ratio = t == null || maxT === minT ? 0.5 : (t - minT) / (maxT - minT);
      const h = 18 + Math.round(ratio * 80);
      const isNow = start && end ? (now >= start && now < end) : false;
      const label = start ? String(start.getHours()).padStart(2, "0") : "";
      const title = start ? `${start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} | ${fmtNum(t, 1)} C` : "";
      return `<div class="hourly-bar${isNow ? " now" : ""}" style="height:${h}px" title="${title}">
        <span>${label}</span>
      </div>`;
    }).join("");

    hourlyChart.innerHTML = `
      <div class="hourly-axis">
        <div>${fmtNum(maxT, 1)} C</div>
        <div>${fmtNum(midT, 1)} C</div>
        <div>${fmtNum(minT, 1)} C</div>
      </div>
      <div class="hourly-bars">${bars}</div>
    `;
    attachHoverGrid(hourlyChart);
  }

  function setWeatherLoading() {
    weatherTemp.textContent = "...";
    weatherHumidity.textContent = "...";
    weatherWind.textContent = "...";
    weatherPrecip.textContent = "...";
    weatherSummary.textContent = "...";
    weatherTmean.textContent = "...";
    weatherPrcpWindow.textContent = "...";
    weatherStations.textContent = "Loading stations...";
    if (weatherHistoryStation) weatherHistoryStation.textContent = "Loading history...";
    if (weatherHistoryTbody) weatherHistoryTbody.innerHTML = "";
  }

  function renderStations(list) {
    lastNearestStations = Array.isArray(list) ? list.slice() : [];
    const visible = lastNearestStations.slice(0, 5);
    if (!visible.length) {
      weatherStations.textContent = "No stations found";
      return;
    }
    weatherStations.innerHTML = visible.map(s => {
      const name = s.name || s.station_id || "station";
      const dist = s.dist_km != null ? `${Number(s.dist_km).toFixed(1)} km` : "";
      const date = s.latest_date && s.latest_date !== "null" ? s.latest_date : "";
      const tmean = s.tmean_c != null ? `${Number(s.tmean_c).toFixed(1)} C` : "";
      const prcp = s.prcp_latest_mm != null ? `${Number(s.prcp_latest_mm).toFixed(1)} mm` : "";
      const lat = Number.isFinite(Number(s.lat)) ? Number(s.lat) : "";
      const lon = Number.isFinite(Number(s.lon)) ? Number(s.lon) : "";
      const isSelected = selectedStationId && s.station_id === selectedStationId;
      const coverage = formatCoverage(s);
      return `<div class="poi-item${isSelected ? " is-selected" : ""}">
        <div class="poi-meta">${escapeHtml(name)} <span class="muted">${dist} ${date ? " - " + date : ""} ${tmean} ${prcp}</span>
          ${coverage ? `<div class="muted">${coverage}</div>` : ""}
        </div>
        <div class="poi-actions">
          <button class="btn btn-small" data-action="station-select" data-station-id="${encodeURIComponent(s.station_id || "")}" data-station-lat="${lat}" data-station-lon="${lon}" data-station-name="${encodeURIComponent(name)}">Select</button>
        </div>
      </div>`;
    }).join("");
  }

  function formatCoverage(station) {
    const total = Number(station.rows_total);
    if (!Number.isFinite(total) || total <= 0) {
      const latest = station.latest_date && station.latest_date !== "null" ? station.latest_date : null;
      return latest ? `History: latest ${latest}` : "No daily history";
    }
    const tmax = Math.round((Number(station.rows_tmax || 0) / total) * 100);
    const tmin = Math.round((Number(station.rows_tmin || 0) / total) * 100);
    const prcp = Math.round((Number(station.rows_prcp || 0) / total) * 100);
    const first = station.first_date && station.first_date !== "null" ? station.first_date : null;
    const last = station.last_date && station.last_date !== "null" ? station.last_date : null;
    const range = (first && last) ? `${first} → ${last}` : "";
    return `Coverage: Tmax ${tmax}% • Tmin ${tmin}% • Prcp ${prcp}%${range ? ` (${range})` : ""}`;
  }

  weatherStations.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-action='station-select']");
    if (!btn) return;
    const stationId = decodeURIComponent(btn.getAttribute("data-station-id") || "");
    const lat = Number(btn.getAttribute("data-station-lat"));
    const lon = Number(btn.getAttribute("data-station-lon"));
    const nameRaw = btn.getAttribute("data-station-name") || "";
    let name = stationId || "Station";
    if (nameRaw) {
      try { name = decodeURIComponent(nameRaw); } catch { name = nameRaw; }
    }
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      focusMapOn(lat, lon);
      const label = `${name} (${lat.toFixed(4)}, ${lon.toFixed(4)})`;
      setPinnedLocation({ lat, lng: lon }, { label, contextType: "station", stationId, skipDetails: true });
      loadDetailsForStation(stationId, name, lat, lon);
    }
  });

  function updateSelectionOverlay(point, station, grid) {
    const features = [];
    if (point && station) {
      features.push({
        type: "Feature",
        geometry: { type: "LineString", coordinates: [[point.lon, point.lat], [station.lon, station.lat]] },
        properties: { kind: "station-link" }
      });
    }
    if (station) {
      features.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [station.lon, station.lat] },
        properties: { kind: "station" }
      });
    }
    if (grid) {
      features.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [grid.lon, grid.lat] },
        properties: { kind: "grid" }
      });
    }
    safeSetData(SRC.selection, { type: "FeatureCollection", features });
  }

  function buildSourceNote({ grid, station, stationName }) {
    const parts = [];
    if (grid && grid.grid_id) {
      parts.push(`Hourly: gridpoint ${grid.grid_id}`);
    }
    if (station) {
      const name = stationName || station.name || station.station_id || "station";
      parts.push(`History: station ${name}`);
    }
    return parts.join(" | ");
  }

  async function updateWeatherFor(lat, lon, modeLabel, force = false) {
    const now = Date.now();
    if (!force && lastWeatherCenter) {
      const dKm = haversineKm(lat, lon, lastWeatherCenter.lat, lastWeatherCenter.lon);
      if ((now - lastWeatherFetchAt) < WEATHER_MIN_MS && dKm < WEATHER_MIN_KM) {
        return;
      }
    }
    lastWeatherFetchAt = now;
    lastWeatherCenter = { lat, lon };

    weatherMode.textContent = modeLabel;
    weatherLocation.textContent = modeLabel === "Pinned" ? "Pinned location" : "View center";
    weatherLatLon.textContent = `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
    setWeatherLoading();

    try {
      const days = window.CONFIG.WEATHER_HISTORY_DAYS || 30;
      const data = await window.API.pointSummary(lat, lon, days, 25);

      let live = null;
      try {
        const liveResp = await window.API.pointLive(lat, lon);
        live = liveResp && liveResp.hourly ? liveResp.hourly : liveResp;
      } catch (e) {
        live = data.hourly || null;
      }

      if (live) {
        weatherTemp.textContent = live.temperature_c != null ? `${fmtNum(live.temperature_c, 1)} C` : "--";
        weatherHumidity.textContent = live.relative_humidity != null ? `${fmtNum(live.relative_humidity, 0)} %` : "--";
        weatherWind.textContent = live.wind_speed_mps != null ? `${fmtNum(live.wind_speed_mps, 1)} m/s` : "--";
        weatherPrecip.textContent = live.precip_prob != null ? `${fmtNum(live.precip_prob, 0)} %` : "--";
        weatherSummary.textContent = live.short_forecast || "--";
      } else {
        weatherTemp.textContent = "--";
        weatherHumidity.textContent = "--";
        weatherWind.textContent = "--";
        weatherPrecip.textContent = "--";
        weatherSummary.textContent = "--";
      }

      const interp = data.interpolated || {};
      weatherTmean.textContent = interp.tmean_c != null ? `${fmtNum(interp.tmean_c, 1)} C` : "--";
      weatherPrcpWindow.textContent = interp.prcp_window_mm != null
        ? `${fmtNum(interp.prcp_window_mm, 1)} mm / ${interp.days ?? days}d`
        : "--";

      renderStations(data.nearest_stations);

      const station = data.nearest_stations && data.nearest_stations[0];
      if (station && station.station_id && weatherHistoryTbody) {
        const range = historyRange(7);
        const hist = await window.API.historyDaily(station.station_id, range.start, range.end);
        const name = station.name || station.station_id;
        if (weatherHistoryStation) {
          const coverage = formatCoverage(station);
          weatherHistoryStation.textContent = `Station: ${name}${coverage ? ` — ${coverage}` : ""}`;
        }
        weatherHistoryTbody.innerHTML = (hist || []).map(r => {
          const tmean = (Number.isFinite(r.tmax_c) && Number.isFinite(r.tmin_c))
            ? (Number(r.tmax_c) + Number(r.tmin_c)) / 2
            : null;
          return `<tr>
            <td>${r.date || ""}</td>
            <td>${fmtNum(r.tmax_c, 1)}</td>
            <td>${fmtNum(r.tmin_c, 1)}</td>
            <td>${fmtNum(r.prcp_mm, 1)}</td>
            <td>${tmean != null ? fmtNum(tmean, 1) : ""}</td>
          </tr>`;
        }).join("") || "<tr><td colspan=\"5\">No history data</td></tr>";
      } else if (weatherHistoryTbody) {
        weatherHistoryTbody.innerHTML = "<tr><td colspan=\"5\">No history data</td></tr>";
        if (weatherHistoryStation) weatherHistoryStation.textContent = "No station history available";
      }
    } catch (e) {
      weatherSummary.textContent = `Error: ${e.message}`;
      weatherStations.textContent = "--";
      if (weatherHistoryTbody) weatherHistoryTbody.innerHTML = "<tr><td colspan=\"5\">History error</td></tr>";
    }
  }

  async function loadDetailsForGrid(gridId, metaText, lat, lon) {
    selectedGridId = gridId;
    selectedContext = { type: "grid", gridId, label: metaText || `grid_id=${gridId}`, lat, lon };
    clearTables();

    setSelectedHeader("grid", metaText || `grid_id=${gridId}`);

    // Hourly forecast
    let forecastTemp = null;
    try {
      const hourly = await window.API.hourlyForecast(gridId, window.CONFIG.FORECAST_LIMIT, 24);
      renderHourlyForecast(hourly);
      if (hourly && hourly.length && Number.isFinite(hourly[0].temperature_c)) {
        forecastTemp = hourly[0].temperature_c;
      }
    } catch (e) {
      renderHourlyForecast([], `Hourly error: ${e.message}`);
      setMlWeatherData(null, null);
    }

    // Daily history
    try {
      const days = Number(historyDays.value || 365);
      const hist = await window.API.historyGridpoint(gridId, days);
      if (hist && hist.length) {
        historyTbody.innerHTML = hist.slice(-400).map(r => {
          return `<tr>
            <td>${r.date || ""}</td>
            <td>${fmtNum(r.tmax_c, 1)}</td>
            <td>${fmtNum(r.tmin_c, 1)}</td>
            <td>${fmtNum(r.prcp_mm, 1)}</td>
          </tr>`;
        }).join("");
        if (days < 365) {
          try {
            const chartHist = await window.API.historyGridpoint(gridId, 365);
            setHistoryData(chartHist);
          } catch (e) {
            setHistoryData(hist);
          }
        } else {
          setHistoryData(hist);
        }
        selectedStationId = null;
        setSelectedHeader("grid", metaText || `grid_id=${gridId}`, buildSourceNote({ grid: { grid_id: gridId } }));
        updateSelectionOverlay({ lat, lon }, null, { lat, lon, grid_id: gridId });
      } else {
        throw new Error("No grid history");
      }
    } catch (e) {
      try {
        const days = Number(historyDays.value || 365);
        const summary = await window.API.pointSummary(lat, lon, days, 25);
        const fallback = await loadHistoryFromNearestStations(summary.nearest_stations, days);
        const station = fallback.station;
        if (station) {
          selectedStationId = station.station_id || null;
          const note = buildSourceNote({ grid: summary.nearest_gridpoint, station, stationName: station.name });
          setSelectedHeader("grid", metaText || `grid_id=${gridId}`, note);
          if (Number.isFinite(Number(station.lat)) && Number.isFinite(Number(station.lon))) {
            updateSelectionOverlay({ lat, lon }, { lat: Number(station.lat), lon: Number(station.lon) }, { lat, lon, grid_id: gridId });
          }
          if (lastNearestStations.length) {
            renderStations(lastNearestStations);
          }
        }
      } catch (err) {
        historyTbody.innerHTML = `<tr><td colspan="4">History error: ${e.message}</td></tr>`;
        setHistoryData([]);
        updateSelectionOverlay({ lat, lon }, null, { lat, lon, grid_id: gridId });
      }
    }

    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      await updateMlWeather("gridpoint", gridId, lat, lon, forecastTemp, gridId, selectedStationId);
    }

    // ML predictions (optional)
    try {
      const preds = await window.API.mlPredictionsLatest(gridId, window.CONFIG.ML_LIMIT);
      mlStatus.textContent = `rows=${preds.length}`;
      mlTbody.innerHTML = preds.map(p => {
        return `<tr>
          <td>${shortIso(p.valid_time)}</td>
          <td>${p.horizon_hours ?? ""}</td>
          <td>${fmtNum(p.risk_score, 3)}</td>
          <td>${p.risk_class ?? ""}</td>
        </tr>`;
      }).join("");
    } catch (e) {
      mlStatus.textContent = `ML not available (or empty): ${e.message}`;
    }
  }

  btnHistoryReload.addEventListener("click", async () => {
    if (!selectedContext) return;
    if (selectedContext.type === "grid") {
      await loadDetailsForGrid(selectedContext.gridId, selectedContext.label, selectedContext.lat, selectedContext.lon);
      return;
    }
    if (selectedContext.type === "station") {
      await loadDetailsForStation(selectedContext.stationId, selectedContext.label, selectedContext.lat, selectedContext.lon);
      return;
    }
    if (selectedContext.type === "pin" || selectedContext.type === "tracked") {
      await loadDetailsForPoint(
        selectedContext.lat,
        selectedContext.lon,
        selectedContext.label,
        selectedContext.stationId,
        selectedContext.type
      );
    }
  });

  // Click gridpoints for details
  map.on("click", "gridpoints-circle", async (e) => {
    if (e && e.originalEvent) e.originalEvent.stopPropagation();
    const f = e.features && e.features[0];
    if (!f) return;

    const gridId = f.properties && (f.properties.grid_id || f.properties.gridId);
    if (!gridId) return;

    const metaText = `grid_id=${gridId} | office=${f.properties.office ?? ""} | grid=${f.properties.grid_x ?? ""},${f.properties.grid_y ?? ""}`;

    // Popup
    new maplibregl.Popup()
      .setLngLat(e.lngLat)
      .setHTML(`<div style="font-size:12px">
        <div><b>${gridId}</b></div>
        <div>office=${f.properties.office ?? ""}</div>
        <div>grid=${f.properties.grid_x ?? ""},${f.properties.grid_y ?? ""}</div>
      </div>`)
      .addTo(map);

    await loadDetailsForGrid(gridId, metaText, e.lngLat.lat, e.lngLat.lng);
    setPinnedLocation(e.lngLat, { skipDetails: true });
  });

  map.on("mouseenter", "gridpoints-circle", () => map.getCanvas().style.cursor = "pointer");
  map.on("mouseleave", "gridpoints-circle", () => {
    map.getCanvas().style.cursor = "";
    clearHover();
  });
  map.on("mousemove", "gridpoints-circle", (e) => {
    const f = e.features && e.features[0];
    if (!f) return;
    const gridId = f.properties && (f.properties.grid_id || f.properties.gridId);
    if (!gridId) return;
    const key = `grid:${gridId}`;
    scheduleHover(key, () => {
      hoverPopup
        .setLngLat(e.lngLat)
        .setHTML(`<div><b>${gridId}</b><div>office=${f.properties.office ?? ""}</div><div>grid=${f.properties.grid_x ?? ""},${f.properties.grid_y ?? ""}</div></div>`)
        .addTo(map);
    });
  });

  map.on("click", "stations-circle", (e) => {
    if (e && e.originalEvent) e.originalEvent.stopPropagation();
    const f = e.features && e.features[0];
    if (!f) return;
    const p = f.properties || {};
    const tmean = p.tmean_c != null ? fmtNum(Number(p.tmean_c), 3) : "";
    const prcp = p.prcp_mm != null ? fmtNum(Number(p.prcp_mm), 3) : "";
    new maplibregl.Popup()
      .setLngLat(e.lngLat)
      .setHTML(`<div style="font-size:12px">
        <div><b>${p.name || p.station_id || "station"}</b></div>
        <div>tmean=${tmean} C</div>
        <div>prcp=${prcp} mm</div>
        <div>latest=${p.latest_date ?? ""}</div>
      </div>`)
      .addTo(map);

    if (p.station_id) {
      loadDetailsForStation(p.station_id, p.name || p.station_id, e.lngLat.lat, e.lngLat.lng);
    }
  });

  map.on("mouseenter", "stations-circle", () => map.getCanvas().style.cursor = "pointer");
  map.on("mouseleave", "stations-circle", () => {
    map.getCanvas().style.cursor = "";
    clearHover();
  });
  map.on("mousemove", "stations-circle", (e) => {
    const f = e.features && e.features[0];
    if (!f) return;
    const p = f.properties || {};
    const stationId = p.station_id || p.stationId;
    if (!stationId) return;
    const key = `station:${stationId}`;
    const tmean = p.tmean_c != null ? fmtNum(Number(p.tmean_c), 3) : "";
    const prcp = p.prcp_mm != null ? fmtNum(Number(p.prcp_mm), 3) : "";
    scheduleHover(key, () => {
      hoverPopup
        .setLngLat(e.lngLat)
        .setHTML(`<div><b>${p.name || stationId}</b><div>tmean=${tmean} C</div><div>prcp=${prcp} mm</div><div>latest=${p.latest_date ?? ""}</div></div>`)
        .addTo(map);
    });
  });

  map.on("click", "stations-all-clusters", (e) => {
    if (!e || !e.features || !e.features[0]) return;
    const feature = e.features[0];
    const clusterId = feature.properties.cluster_id;
    const src = map.getSource(SRC.stationsAll);
    if (!src || !src.getClusterExpansionZoom) return;
    src.getClusterExpansionZoom(clusterId, (err, zoom) => {
      if (err) return;
      map.easeTo({
        center: feature.geometry.coordinates,
        zoom: Math.max(zoom, map.getZoom() + 1),
        duration: 600
      });
    });
  });

  map.on("click", "stations-all-unclustered", (e) => {
    if (e && e.originalEvent) e.originalEvent.stopPropagation();
    if (!e || !e.lngLat) return;
    const f = e.features && e.features[0];
    const p = f && f.properties ? f.properties : {};
    const stationId = p.station_id || p.stationId;
    const name = p.name || stationId || "station";
    const tmean = p.tmean_c != null ? fmtNum(Number(p.tmean_c), 3) : "";
    const prcp = p.prcp_mm != null ? fmtNum(Number(p.prcp_mm), 3) : "";
    new maplibregl.Popup()
      .setLngLat(e.lngLat)
      .setHTML(`<div style="font-size:12px">
        <div><b>${name}</b></div>
        <div>tmean=${tmean} C</div>
        <div>prcp=${prcp} mm</div>
        <div>latest=${p.latest_date ?? ""}</div>
      </div>`)
      .addTo(map);
    if (stationId) {
      loadDetailsForStation(stationId, name, e.lngLat.lat, e.lngLat.lng);
    }
  });

  map.on("mouseenter", "stations-all-clusters", () => map.getCanvas().style.cursor = "pointer");
  map.on("mouseleave", "stations-all-clusters", () => {
    map.getCanvas().style.cursor = "";
    clearHover();
  });
  map.on("mouseenter", "stations-all-unclustered", () => map.getCanvas().style.cursor = "pointer");
  map.on("mouseleave", "stations-all-unclustered", () => {
    map.getCanvas().style.cursor = "";
    clearHover();
  });
  map.on("mousemove", "stations-all-unclustered", (e) => {
    const f = e.features && e.features[0];
    if (!f) return;
    const p = f.properties || {};
    const stationId = p.station_id || p.stationId;
    if (!stationId) return;
    const key = `station:${stationId}`;
    const tmean = p.tmean_c != null ? fmtNum(Number(p.tmean_c), 3) : "";
    const prcp = p.prcp_mm != null ? fmtNum(Number(p.prcp_mm), 3) : "";
    scheduleHover(key, () => {
      hoverPopup
        .setLngLat(e.lngLat)
        .setHTML(`<div><b>${p.name || stationId}</b><div>tmean=${tmean} C</div><div>prcp=${prcp} mm</div><div>latest=${p.latest_date ?? ""}</div></div>`)
        .addTo(map);
    });
  });

  map.on("click", "pois-circle", (e) => {
    if (e && e.originalEvent) e.originalEvent.stopPropagation();
    if (e && e.lngLat) {
      const f = e.features && e.features[0];
      const p = f && f.properties ? f.properties : {};
      const label = p.name
        ? `${p.name} (${e.lngLat.lat.toFixed(4)}, ${e.lngLat.lng.toFixed(4)})`
        : `Tracked ${e.lngLat.lat.toFixed(4)}, ${e.lngLat.lng.toFixed(4)}`;
      setPinnedLocation(e.lngLat, { label, contextType: "tracked" });
    }
  });
  map.on("mouseenter", "pois-circle", () => map.getCanvas().style.cursor = "pointer");
  map.on("mouseleave", "pois-circle", () => {
    map.getCanvas().style.cursor = "";
    clearHover();
  });
  map.on("mousemove", "pois-circle", (e) => {
    const f = e.features && e.features[0];
    if (!f) return;
    const p = f.properties || {};
    const id = p.id;
    const name = p.name || "Tracked";
    const key = `tracked:${id ?? name}`;
    scheduleHover(key, () => {
      hoverPopup
        .setLngLat(e.lngLat)
        .setHTML(`<div><b>${name}</b><div>${e.lngLat.lat.toFixed(4)}, ${e.lngLat.lng.toFixed(4)}</div></div>`)
        .addTo(map);
    });
  });

  // Click anywhere to pin and load weather at that point
  map.on("click", (e) => {
    if (e && e.lngLat) {
      setPinnedLocation(e.lngLat, { contextType: "pin" });
    }
  });

})();

