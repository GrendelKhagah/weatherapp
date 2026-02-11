/**
 * Create chart renderers for hourly, history, and ML forecast views.
 */
function createCharts({ hourlyChart, historyChart, mlForecastChart }, { fmtNum, formatShortDate }) {
  /** Attach crosshair + tooltip behavior to a chart container. */
  function attachHoverGrid(container) {
    if (!container) return;
    let xLine = container.querySelector(".chart-crosshair-x");
    let yLine = container.querySelector(".chart-crosshair-y");
    let tooltip = container.querySelector(".chart-tooltip");

    if (!xLine) {
      xLine = document.createElement("div");
      xLine.className = "chart-crosshair-x";
      container.appendChild(xLine);
    }
    if (!yLine) {
      yLine = document.createElement("div");
      yLine.className = "chart-crosshair-y";
      container.appendChild(yLine);
    }
    if (!tooltip) {
      tooltip = document.createElement("div");
      tooltip.className = "chart-tooltip";
      container.appendChild(tooltip);
    }

    if (container.dataset.hoverGridReady) return;
    container.dataset.hoverGridReady = "true";

    container.addEventListener("mousemove", (e) => {
      const xLineEl = container.querySelector(".chart-crosshair-x");
      const yLineEl = container.querySelector(".chart-crosshair-y");
      const tooltipEl = container.querySelector(".chart-tooltip");
      if (!xLineEl || !yLineEl || !tooltipEl) return;

      const rect = container.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      container.classList.add("chart-hover");
      let xLinePos = Math.max(0, Math.min(rect.width, x));
      let yLinePos = Math.max(0, Math.min(rect.height, y));

      let tooltipHtml = "";
      const hoverData = container._hoverData;
      if (hoverData && typeof hoverData.getTooltip === "function") {
        const result = hoverData.getTooltip({ x, y, rect });
        if (result && result.html) {
          tooltipHtml = result.html;
          if (Number.isFinite(result.x)) xLinePos = Math.max(0, Math.min(rect.width, result.x));
          if (Number.isFinite(result.y)) yLinePos = Math.max(0, Math.min(rect.height, result.y));
        }
      }

      xLineEl.style.top = `${yLinePos}px`;
      yLineEl.style.left = `${xLinePos}px`;
      if (tooltipHtml) {
        tooltipEl.innerHTML = tooltipHtml;
        tooltipEl.style.display = "block";
        const tw = tooltipEl.offsetWidth || 120;
        const th = tooltipEl.offsetHeight || 40;
        let left = xLinePos + 12;
        let top = yLinePos + 12;
        if (left + tw > rect.width) left = xLinePos - tw - 12;
        if (top + th > rect.height) top = yLinePos - th - 12;
        tooltipEl.style.left = `${Math.max(0, left)}px`;
        tooltipEl.style.top = `${Math.max(0, top)}px`;
      } else {
        tooltipEl.style.display = "none";
      }
    });

    container.addEventListener("mouseleave", () => {
      const tooltipEl = container.querySelector(".chart-tooltip");
      container.classList.remove("chart-hover");
      if (tooltipEl) tooltipEl.style.display = "none";
    });
  }

  /** Render the hourly temperature bar chart. */
  function renderHourlyChart(rows) {
    if (!rows || rows.length === 0) {
      hourlyChart.innerHTML = "";
      hourlyChart._hoverData = null;
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
    hourlyChart._hoverData = {
      getTooltip: ({ x, rect }) => {
        const barsEl = hourlyChart.querySelector(".hourly-bars");
        if (!barsEl) return null;
        const barsRect = barsEl.getBoundingClientRect();
        const localX = x + rect.left - barsRect.left;
        if (localX < 0 || localX > barsRect.width) return null;
        const denom = Math.max(1, rows.length - 1);
        const idx = Math.round((localX / barsRect.width) * denom);
        const row = rows[idx];
        if (!row) return null;
        const start = row.start_time ? new Date(row.start_time) : null;
        const dateLabel = start
          ? start.toLocaleDateString([], { weekday: "short", month: "short", day: "numeric" })
          : "--";
        const timeLabel = start
          ? start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
          : "--";
        const temp = Number.isFinite(row.temperature_c) ? `${fmtNum(row.temperature_c, 1)} C` : "--";
        const prcp = Number.isFinite(row.precip_prob) ? `${fmtNum(row.precip_prob, 0)}%` : "--";
        const wind = Number.isFinite(row.wind_speed_mps) ? `${fmtNum(row.wind_speed_mps, 1)} m/s` : "--";
        return {
          html: `<div><strong>${dateLabel} ${timeLabel}</strong></div><div>Temp: ${temp}</div><div>Precip: ${prcp}</div><div>Wind: ${wind}</div>`,
          x: barsRect.left - rect.left + (idx / denom) * barsRect.width
        };
      }
    };
  }

  /** Render the historical min/max temperature chart. */
  function renderHistoryChart(rows) {
    if (!historyChart) return;
    historyChart.innerHTML = "";
    const chartRows = (rows || []).filter(r => r && (Number.isFinite(r.tmin_c) || Number.isFinite(r.tmax_c)));
    if (!chartRows.length) {
      historyChart._hoverData = null;
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
      historyChart._hoverData = null;
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

    historyChart._hoverData = {
      getTooltip: ({ x }) => {
        const xClamped = Math.max(pad.left, Math.min(pad.left + chartW, x));
        const ratio = (xClamped - pad.left) / chartW;
        const idx = Math.round(ratio * denom);
        const row = clippedRows[idx];
        if (!row) return null;
        const dateLabel = row.date ? formatShortDate(row.date) : "--";
        const min = Number.isFinite(row.tmin_c) ? `${fmtNum(row.tmin_c, 1)} C` : "--";
        const max = Number.isFinite(row.tmax_c) ? `${fmtNum(row.tmax_c, 1)} C` : "--";
        const mean = Number.isFinite(row.tmean_c) ? `${fmtNum(row.tmean_c, 1)} C` : null;
        const prcp = Number.isFinite(row.prcp_mm) ? `${fmtNum(row.prcp_mm, 1)} mm` : "--";
        return {
          html: `<div><strong>${dateLabel}</strong></div><div>Min: ${min}</div><div>Max: ${max}</div>${mean ? `<div>Mean: ${mean}</div>` : ""}<div>Precip: ${prcp}</div>`,
          x: pad.left + (idx / denom) * chartW
        };
      }
    };
  }

  /** Render the ML forecast chart with optional NWS comparison. */
  function renderMlForecastChart(rows, nwsRows = []) {
    if (!mlForecastChart) return;
    mlForecastChart.innerHTML = "";
    const list = Array.isArray(rows) ? rows.slice(0, 7) : [];
    if (!list.length) {
      mlForecastChart._hoverData = null;
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
    const denom = Math.max(1, n - 1);

    const yFor = (t) => pad.top + (1 - (t - minT) / (maxT - minT || 1)) * innerH;

    ctx.strokeStyle = "rgba(255,255,255,0.2)";
    ctx.beginPath();
    ctx.moveTo(pad.left, yFor(minT));
    ctx.lineTo(pad.left + innerW, yFor(minT));
    ctx.moveTo(pad.left, yFor(maxT));
    ctx.lineTo(pad.left + innerW, yFor(maxT));
    ctx.stroke();

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

    ctx.strokeStyle = "rgba(255,90,90,0.9)";
    ctx.beginPath();
    list.forEach((r, idx) => {
      if (!Number.isFinite(r.tmax_c)) return;
      const x = pad.left + (idx / Math.max(1, n - 1)) * innerW;
      const y = yFor(r.tmax_c);
      if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();

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

    mlForecastChart._hoverData = {
      getTooltip: ({ x }) => {
        const xClamped = Math.max(pad.left, Math.min(pad.left + innerW, x));
        const ratio = (xClamped - pad.left) / innerW;
        const idx = Math.round(ratio * denom);
        const row = list[idx];
        if (!row) return null;
        const base = row.as_of_date ? new Date(`${row.as_of_date}T00:00:00Z`) : null;
        const day = base ? new Date(base.getTime() + (row.horizon_hours || 0) * 3600000) : null;
        const label = day
          ? day.toLocaleDateString([], { weekday: "short", month: "short", day: "numeric" })
          : "--";
        const min = Number.isFinite(row.tmin_c) ? `${fmtNum(row.tmin_c, 1)} C` : "--";
        const max = Number.isFinite(row.tmax_c) ? `${fmtNum(row.tmax_c, 1)} C` : "--";
        const mean = Number.isFinite(row.tmean_c) ? `${fmtNum(row.tmean_c, 1)} C` : "--";
        let nwsMean = "--";
        const nwsRow = nwsRows && nwsRows[idx] ? nwsRows[idx] : null;
        if (nwsRow && Number.isFinite(nwsRow.tmin_c) && Number.isFinite(nwsRow.tmax_c)) {
          nwsMean = `${fmtNum((nwsRow.tmin_c + nwsRow.tmax_c) / 2, 1)} C`;
        }
        const conf = Number.isFinite(row.confidence) ? `${Math.round(row.confidence * 100)}%` : "--";
        return {
          html: `<div><strong>${label}</strong></div><div>Min: ${min}</div><div>Max: ${max}</div><div>Mean: ${mean}</div><div>NWS Mean: ${nwsMean}</div><div>Confidence: ${conf}</div>`,
          x: pad.left + (idx / denom) * innerW
        };
      }
    };
  }

  return {
    attachHoverGrid,
    renderHourlyChart,
    renderHistoryChart,
    renderMlForecastChart,
  };
}

window.App = window.App || {};
window.App.charts = { createCharts };
