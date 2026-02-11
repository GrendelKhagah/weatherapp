/**
 * Create handlers for loading and displaying station history.
 */
function createHistoryHandlers({ historyTbody }, { fmtNum, historyRange }, { setHistoryData }) {
  /** Fetch daily history for one station. */
  async function fetchHistoryForStation(stationId, days) {
    const { start, end } = historyRange(days);
    return window.API.historyDaily(stationId, start, end);
  }

  /** Load history for a station and update table + chart. */
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

  /** Try multiple nearby stations until history data is found. */
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

  return {
    fetchHistoryForStation,
    loadHistoryForStation,
    loadHistoryFromNearestStations,
  };
}

window.App = window.App || {};
window.App.history = { createHistoryHandlers };
