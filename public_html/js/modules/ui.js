/** Map API status labels to CSS classes. */
function statusClass(status) {
  if (!status) return "unknown";
  const s = String(status).toLowerCase();
  if (s === "ok") return "ok";
  if (s === "degraded") return "degraded";
  if (s === "down") return "down";
  if (s === "no-data") return "no-data";
  return "unknown";
}

/**
 * Create UI handlers for utility counts, health, and panels.
 */
function createUiHandlers({ utilStations, utilStationsWithData, utilGridpoints, utilTrackedPoints, utilApiHealth }) {
  /** Refresh station/gridpoint/point counts in the sidebar. */
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

  /** Refresh external API health badges (NOAA/NWS). */
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

  /** Enable collapsible panels for the UI sections. */
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

  return {
    refreshUtilityCounts,
    refreshExternalApiHealth,
    initPanelCollapsing,
  };
}

window.App = window.App || {};
window.App.ui = { createUiHandlers };
