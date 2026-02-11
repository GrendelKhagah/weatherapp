/** Get an element by id. */
const el = (id) => document.getElementById(id);

/** Format a number with fixed decimals for display. */
function fmtNum(v, digits = 1) {
  if (v === null || v === undefined) return "";
  if (Number.isFinite(v)) return v.toFixed(digits);
  return String(v);
}

/** Format YYYY-MM-DD into M/D/YYYY for compact labels. */
function formatShortDate(value) {
  if (!value) return "--";
  const m = String(value).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return String(value);
  return `${Number(m[2])}/${Number(m[3])}/${m[1]}`;
}

/** Convert an ISO timestamp into a short M-D HH:mm label. */
function shortIso(s) {
  if (!s) return "";
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) {
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    const hh = String(d.getHours()).padStart(2, "0");
    const min = String(d.getMinutes()).padStart(2, "0");
    return `${mm}-${dd} ${hh}:${min}`;
  }
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
  return m ? `${m[2]}-${m[3]} ${m[4]}:${m[5]}` : s;
}

/** Escape HTML special characters to prevent injection. */
function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** Copy text to clipboard with a safe fallback. */
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

/** Temporarily swap a button label to confirm an action. */
function flashButton(btn, label) {
  if (!btn) return;
  const prev = btn.textContent;
  btn.textContent = label;
  setTimeout(() => {
    btn.textContent = prev;
  }, 1200);
}

/** Compute a start/end date range for history requests. */
function historyRange(days) {
  const end = new Date();
  end.setDate(end.getDate() - 1);
  const start = new Date(end);
  start.setDate(start.getDate() - (days - 1));
  const fmt = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  return { start: fmt(start), end: fmt(end) };
}

/** Compute distance in km between two lat/lon points. */
function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

window.App = window.App || {};
window.App.utils = {
  el,
  fmtNum,
  formatShortDate,
  shortIso,
  escapeHtml,
  copyToClipboard,
  flashButton,
  historyRange,
  haversineKm,
};
