/** Compute a sortable timestamp key for ML forecast rows. */
function mlForecastDateKey(r) {
  if (!r) return null;
  if (r.date) {
    const d = new Date(r.date);
    return Number.isNaN(d.getTime()) ? null : d.getTime();
  }
  if (!r.as_of_date) return null;
  const base = new Date(`${r.as_of_date}T00:00:00Z`);
  if (Number.isNaN(base.getTime())) return null;
  const h = Number(r.horizon_hours || 0);
  return base.getTime() + h * 3600000;
}

/** Sort ML forecast rows by date/horizon. */
function sortMlForecastRows(rows) {
  const list = Array.isArray(rows) ? rows.slice() : [];
  list.sort((a, b) => {
    const ak = mlForecastDateKey(a);
    const bk = mlForecastDateKey(b);
    if (ak == null && bk == null) return 0;
    if (ak == null) return 1;
    if (bk == null) return -1;
    return ak - bk;
  });
  return list;
}

/** Filter ML forecast rows to the next N days. */
function filterMlForecastWindow(rows, maxDays = 10) {
  const list = sortMlForecastRows(rows);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const end = new Date(today.getTime());
  end.setDate(end.getDate() + maxDays);
  return list.filter(r => {
    const key = mlForecastDateKey(r);
    if (key == null) return false;
    return key >= today.getTime() && key <= end.getTime();
  });
}

/**
 * Create ML forecast handlers for the UI widgets.
 */
function createMlHandlers(
  {
    elements,
    helpers,
    getHistoryRows,
    getNearestStations,
    setMlForecastData,
  }
) {
  const {
    mlWeatherStatus,
    mlWeatherTodayDate,
    mlWeatherTomorrowDate,
    mlWeatherTodayTemp,
    mlWeatherTomorrowTemp,
    mlWeatherTodayPrcp,
    mlWeatherTomorrowPrcp,
    mlWeatherTodayDelta,
    mlWeatherTomorrowDelta,
    mlWeatherModel,
    mlForecastStatus,
    mlForecastLegend,
    mlForecastModel,
    mlForecastTbody,
  } = elements;

  const { fmtNum, formatShortDate } = helpers;

  /** Safely set text content on an element. */
  function setText(el, value) {
    if (!el) return;
    el.textContent = value;
  }

  const sortForecast = sortMlForecastRows;
  let lastForecastRows = [];
  let lastNwsDaily = [];

  /** Build a model identity string from name/detail. */
  function modelKey(row) {
    if (!row) return "";
    const name = row.model_name || "model";
    const detail = row.model_detail || "";
    return detail ? `${name}::${detail}` : name;
  }

  /** Normalize a row into a day key for grouping. */
  function dayKey(row) {
    if (!row) return "";
    const base = row.as_of_date ? new Date(`${row.as_of_date}T00:00:00Z`) : null;
    const day = base ? new Date(base.getTime() + (row.horizon_hours || 0) * 3600000) : null;
    return day ? day.toISOString().slice(0, 10) : "";
  }

  /** Read current model selection from the UI. */
  function getSelectedModel() {
    if (!mlForecastModel) return "all";
    return mlForecastModel.value || "all";
  }

  /** Filter rows to the selected model. */
  function filterRowsByModel(rows) {
    const list = Array.isArray(rows) ? rows : [];
    const key = getSelectedModel();
    if (key === "all") return list;
    return list.filter(r => modelKey(r) === key);
  }

  /** Collapse multiple rows into a per-day summary. */
  function collapseByDay(rows) {
    const list = Array.isArray(rows) ? rows : [];
    const grouped = new Map();
    list.forEach(r => {
      const key = dayKey(r);
      if (!key) return;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key).push(r);
    });

    const out = [];
    grouped.forEach((items, key) => {
      let tmin = 0, tmax = 0, tmean = 0, prcp = 0, delta = 0, conf = 0;
      let tminN = 0, tmaxN = 0, tmeanN = 0, prcpN = 0, deltaN = 0, confN = 0;
      items.forEach(r => {
        if (Number.isFinite(r.tmin_c)) { tmin += r.tmin_c; tminN += 1; }
        if (Number.isFinite(r.tmax_c)) { tmax += r.tmax_c; tmaxN += 1; }
        if (Number.isFinite(r.tmean_c)) { tmean += r.tmean_c; tmeanN += 1; }
        if (Number.isFinite(r.prcp_mm)) { prcp += r.prcp_mm; prcpN += 1; }
        if (Number.isFinite(r.delta_c)) { delta += r.delta_c; deltaN += 1; }
        if (Number.isFinite(r.confidence)) { conf += r.confidence; confN += 1; }
      });
      out.push({
        day: key,
        tmin_c: tminN ? tmin / tminN : null,
        tmax_c: tmaxN ? tmax / tmaxN : null,
        tmean_c: tmeanN ? tmean / tmeanN : null,
        prcp_mm: prcpN ? prcp / prcpN : null,
        delta_c: deltaN ? delta / deltaN : null,
        confidence: confN ? conf / confN : null,
      });
    });

    out.sort((a, b) => (a.day || "").localeCompare(b.day || ""));
    return out;
  }

  /** Render the ML forecast table rows. */
  function renderForecastTable(rows, nwsDaily) {
    if (!mlForecastTbody) return;
    const filtered = filterRowsByModel(rows);
    const collapsed = collapseByDay(filtered);
    const nwsByDay = new Map();
    (nwsDaily || []).forEach(d => {
      const key = (d.day || "").slice(0, 10);
      if (key) nwsByDay.set(key, d);
    });

    mlForecastTbody.innerHTML = collapsed.map(r => {
      const nws = nwsByDay.get(r.day) || {};
      return `<tr>
        <td>${r.day}</td>
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

  /** Render the ML "today" and "tomorrow" summary cards. */
  function setMlWeatherData(pred, forecastTemp) {
    if (!pred) {
      setText(mlWeatherStatus, "No ML prediction yet.");
      setText(mlWeatherTodayDate, "--");
      setText(mlWeatherTomorrowDate, "--");
      setText(mlWeatherTodayTemp, "--");
      setText(mlWeatherTomorrowTemp, "--");
      setText(mlWeatherTodayPrcp, "--");
      setText(mlWeatherTomorrowPrcp, "--");
      setText(mlWeatherTodayDelta, "--");
      setText(mlWeatherTomorrowDelta, "--");
      setText(mlWeatherModel, "--");
      return;
    }
    const asOf = pred.date || pred.as_of_date || "";
    const asOfLabel = asOf ? formatShortDate(asOf) : "--";
    setText(mlWeatherStatus, asOf ? `For ${asOfLabel}` : "ML prediction");
    setText(mlWeatherTodayDate, asOfLabel);
    setText(mlWeatherTomorrowDate, "--");
    setText(mlWeatherTodayTemp, pred.tmean_c != null ? `${fmtNum(pred.tmean_c, 1)} C` : "--");
    setText(mlWeatherTomorrowTemp, "--");
    setText(mlWeatherTodayPrcp, pred.prcp_mm != null ? `${fmtNum(pred.prcp_mm, 1)} mm` : "--");
    setText(mlWeatherTomorrowPrcp, "--");
    if (Number.isFinite(forecastTemp) && pred.tmean_c != null) {
      const delta = Number(pred.tmean_c) - Number(forecastTemp);
      setText(mlWeatherTodayDelta, `${fmtNum(delta, 1)} C`);
    } else {
      setText(mlWeatherTodayDelta, "--");
    }
    setText(mlWeatherTomorrowDelta, "--");
    const modelLabel = pred.model_detail
      ? `${pred.model_name || "model"} (${pred.model_detail})`
      : (pred.model_name || "knn-distance");
    setText(mlWeatherModel, modelLabel);
  }

  /** Pick the best available as-of date for ML requests. */
  function getMlAsOfDate() {
    const historyRows = getHistoryRows();
    if (Array.isArray(historyRows) && historyRows.length) {
      const last = historyRows[historyRows.length - 1];
      if (last && last.date) return last.date;
    }
    const nearest = getNearestStations();
    if (Array.isArray(nearest) && nearest.length) {
      const d = nearest[0]?.latest_date;
      if (d && d !== "null") return d;
    }
    return null;
  }

  /** Fetch ML prediction + forecast and update the UI. */
  async function updateMlWeather(sourceType, sourceId, lat, lon, forecastTemp, gridId, fallbackStationId) {
    setText(mlWeatherStatus, "Updating ML...");
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
          const latestSourceType = gridId ? "gridpoint" : sourceType;
          const latestSourceId = gridId || sourceId;
          const pred = await window.API.mlWeatherLatest(latestSourceType, latestSourceId, lat, lon);
          setMlWeatherData(pred, forecastTemp);
        } catch (finalErr) {
          setText(mlWeatherStatus, `ML unavailable: ${finalErr.message}`);
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
        forecast = await window.API.mlWeatherForecast(forecastSourceType, forecastSourceId, lat, lon, 11);
      } catch (err) {
        forecastErr = err;
      }

      if ((!forecast || !forecast.length) && fallbackStationId && forecastSourceType !== "station") {
        try {
          forecast = await window.API.mlWeatherForecast("station", fallbackStationId, lat, lon, 11);
          usedFallback = true;
          forecastErr = null;
        } catch (err) {
          if (!forecastErr) forecastErr = err;
        }
      }

      if (forecastErr && (!forecast || !forecast.length)) {
        throw forecastErr;
      }

      const nwsDaily = gridId ? await window.API.dailyForecast(gridId, 11) : [];
      const sortedForecast = filterMlForecastWindow(forecast, 10);
      if (mlForecastStatus) {
        if (sortedForecast && sortedForecast.length) {
          const first = sortedForecast[0];
          const last = sortedForecast[sortedForecast.length - 1];
          const startDate = first?.as_of_date ? formatShortDate(first.as_of_date) : "--";
          let endDate = "--";
          if (last?.as_of_date) {
            const base = new Date(`${last.as_of_date}T00:00:00Z`);
            const day = new Date(base.getTime() + (last.horizon_hours || 0) * 3600000);
            endDate = formatShortDate(day.toISOString().slice(0, 10));
          }
          const confidences = sortedForecast
            .map(r => r && Number.isFinite(r.confidence) ? r.confidence : null)
            .filter(v => v != null);
          const avgConfidence = confidences.length
            ? Math.round((confidences.reduce((a, b) => a + b, 0) / confidences.length) * 100)
            : null;
          const confidenceLabel = avgConfidence != null ? `${avgConfidence}%` : "--";
          const fallbackNote = usedFallback ? " • station fallback" : "";
          mlForecastStatus.textContent = `Machine Learning Forecast, Date ${startDate} - ${endDate}, confidence ${confidenceLabel}${fallbackNote}`.trim();
        } else {
          mlForecastStatus.textContent = "No ML forecast yet.";
        }
      }

      if (mlForecastLegend) {
        mlForecastLegend.innerHTML = `
          <span class="legend-item"><span class="legend-line min"></span>Min</span>
          <span class="legend-item"><span class="legend-line max"></span>Max</span>
          <span class="legend-item"><span class="legend-line mean"></span>Mean</span>
          <span class="legend-item"><span class="legend-line nws"></span>NWS Mean</span>
        `;
      }

      lastForecastRows = sortedForecast || [];
      lastNwsDaily = nwsDaily || [];
      renderForecastTable(lastForecastRows, lastNwsDaily);
      setMlForecastData(sortedForecast || [], nwsDaily || []);

      if (sortedForecast && sortedForecast.length) {
        const byH = new Map((sortedForecast || []).map(r => [Number(r.horizon_hours || 0), r]));
        const todayRow = byH.get(0) || sortedForecast[0];
        const tomorrowRow = byH.get(24) || sortedForecast[1];
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

        const modelLabel = sortedForecast[0]?.model_detail
          ? `${sortedForecast[0]?.model_name || "model"} (${sortedForecast[0]?.model_detail})`
          : (sortedForecast[0]?.model_name || "model");
        setText(mlWeatherModel, modelLabel);
      }
    } catch (e) {
      if (mlForecastStatus) mlForecastStatus.textContent = `ML forecast unavailable: ${e.message}`;
      setMlForecastData([]);
    }
  }

  if (mlForecastModel) {
    mlForecastModel.addEventListener("change", () => {
      renderForecastTable(lastForecastRows, lastNwsDaily);
    });
  }

  return {
    setMlWeatherData,
    updateMlWeather,
  };
}

window.App = window.App || {};
window.App.ml = {
  createMlHandlers,
  sortMlForecastRows,
  mlForecastDateKey,
};
