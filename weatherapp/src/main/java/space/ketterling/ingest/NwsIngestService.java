package space.ketterling.ingest;

import java.time.Instant;
import org.slf4j.MDC;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import space.ketterling.config.AppConfig;
import space.ketterling.db.AlertRepo;
import space.ketterling.db.ForecastRepo;
import space.ketterling.db.GridpointRepo;
import space.ketterling.db.IngestLogRepo;
import space.ketterling.db.TrackedPointRepo;
import space.ketterling.nws.NwsClient;

/**
 * Handles ingesting NWS data into the database (gridpoints, hourly, alerts).
 */
public class NwsIngestService {
    private static final Logger log = LoggerFactory.getLogger(NwsIngestService.class);

    private final AppConfig cfg;
    private final ObjectMapper om;
    private final NwsClient nws;
    private final IngestLogRepo logRepo;
    private final GridpointRepo gridRepo;
    private final ForecastRepo forecastRepo;
    private final AlertRepo alertRepo;
    private final TrackedPointRepo trackedPointRepo;

    /**
     * Builds the ingest service with required repos and HTTP client.
     */
    public NwsIngestService(AppConfig cfg,
            ObjectMapper om,
            NwsClient nws,
            IngestLogRepo logRepo,
            GridpointRepo gridRepo,
            ForecastRepo forecastRepo,
            AlertRepo alertRepo,
            TrackedPointRepo trackedPointRepo) {
        this.cfg = cfg;
        this.om = om;
        this.nws = nws;
        this.logRepo = logRepo;
        this.gridRepo = gridRepo;
        this.forecastRepo = forecastRepo;
        this.alertRepo = alertRepo;
        this.trackedPointRepo = trackedPointRepo;
    }

    /**
     * Collects unique tracked points from config and the database.
     */
    private List<double[]> getTrackedPoints() {
        List<double[]> out = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();

        try {
            var rows = trackedPointRepo == null ? List.<TrackedPointRepo.TrackedPoint>of() : trackedPointRepo.list();
            if (rows != null) {
                for (var r : rows) {
                    String key = String.format("%.4f,%.4f", r.lat(), r.lon());
                    if (seen.add(key)) {
                        out.add(new double[] { r.lat(), r.lon() });
                    }
                }
            }
        } catch (Exception ignored) {
        }

        for (double[] pt : cfg.trackedPoints()) {
            String key = String.format("%.4f,%.4f", pt[0], pt[1]);
            if (seen.add(key)) {
                out.add(new double[] { pt[0], pt[1] });
            }
        }

        return out;
    }

    // -------------------------
    // JOB 1: Refresh gridpoints
    // -------------------------
    /**
     * Pulls fresh gridpoint data from NWS for each tracked point.
     */
    public void refreshGridpoints() throws Exception {
        List<double[]> tracked = getTrackedPoints();
        log.info("Starting job: refreshGridpoints (tracked points={})", tracked.size());
        UUID runId = logRepo.startRun("nws_gridpoint_refresh");
        MDC.put("runId", runId.toString());
        int ok = 0;
        int fail = 0;

        try {
            for (double[] pt : tracked) {
                double lat = pt[0];
                double lon = pt[1];
                String endpoint = "https://api.weather.gov/points/" + lat + "," + lon;

                try {
                    long t0 = System.currentTimeMillis();
                    JsonNode root = nws.points(lat, lon);
                    int ms = (int) (System.currentTimeMillis() - t0);

                    JsonNode props = root.path("properties");
                    String office = props.path("gridId").asText(null);
                    int gridX = props.path("gridX").asInt();
                    int gridY = props.path("gridY").asInt();
                    String gridDataUrl = props.path("forecastGridData").asText(null);
                    String hourlyUrl = props.path("forecastHourly").asText(null);

                    if (office == null || gridDataUrl == null || hourlyUrl == null) {
                        throw new IllegalStateException("Missing NWS point fields for " + lat + "," + lon);
                    }

                    String gridId = office + ":" + gridX + "," + gridY;

                    gridRepo.upsertGridpoint(gridId, office, gridX, gridY, lat, lon, gridDataUrl, hourlyUrl);

                    logRepo.logEvent(runId, "NWS", endpoint, 200, ms, null, Map.of());
                    ok++;
                    log.info("Refreshed gridpoint {} ({}:{},{})", gridId, office, gridX, gridY);
                } catch (Exception e) {
                    logRepo.logEvent(runId, "NWS", endpoint, null, null, e.getMessage(), Map.of());
                    fail++;
                    log.warn("Gridpoint refresh failed for {},{}: {}", lat, lon, e.getMessage());
                }
            }

            logRepo.finishRun(runId, fail == 0, "ok=" + ok + " fail=" + fail);
            log.info("Finished refreshGridpoints: ok={} fail={}", ok, fail);
        } catch (Exception outer) {
            logRepo.finishRun(runId, false, "fatal: " + outer.getMessage());
            throw outer;
        } finally {
            MDC.remove("runId");
        }
    }

    // -------------------------
    // JOB 2: Hourly forecasts
    // -------------------------
    /**
     * Downloads hourly forecasts from NWS and writes them to the DB.
     */
    public void ingestHourlyForecasts() throws Exception {
        log.info("Starting job: ingestHourlyForecasts");
        UUID runId = logRepo.startRun("nws_hourly_forecast");
        MDC.put("runId", runId.toString());
        int ok = 0;
        int fail = 0;

        try {
            List<GridpointRepo.GridpointRow> gps = gridRepo.listGridpointsForHourly();
            if (gps.isEmpty()) {
                log.warn("No gridpoints available for hourly ingest. Run refreshGridpoints first.");
                logRepo.finishRun(runId, false, "no gridpoints available");
                return;
            }
            for (var gp : gps) {
                String gridId = gp.gridId();
                String url = gp.forecastHourlyUrl();

                try {
                    long t0 = System.currentTimeMillis();
                    JsonNode root = nws.forecastHourly(url);
                    int ms = (int) (System.currentTimeMillis() - t0);

                    JsonNode props = root.path("properties");
                    Instant issuedAt = parseInstant(props.path("updated").asText(null));

                    JsonNode periods = props.path("periods");
                    if (!periods.isArray())
                        throw new IllegalStateException("No periods array");

                    int written = 0;
                    for (JsonNode p : periods) {
                        Instant start = parseInstant(p.path("startTime").asText(null));
                        Instant end = parseInstant(p.path("endTime").asText(null));
                        if (start == null || end == null)
                            continue;

                        Double tempC = toCelsius(p);
                        Double windSpeed = parseWindSpeedMps(p.path("windSpeed").asText(null));
                        Double windGust = parseWindSpeedMps(p.path("windGust").asText(null)); // sometimes missing
                        Double windDir = cardinalToDegrees(p.path("windDirection").asText(null));

                        Double precip = parsePrecipProb01(p.path("probabilityOfPrecipitation"));
                        Double rh = parseValueNode(p.path("relativeHumidity"));

                        String shortForecast = p.path("shortForecast").asText(null);

                        String raw = om.writeValueAsString(p);

                        forecastRepo.upsertHourly(
                                gridId, start, end,
                                tempC, windSpeed, windGust, windDir,
                                precip, rh,
                                shortForecast,
                                issuedAt,
                                raw);
                        written++;
                    }

                    logRepo.logEvent(runId, "NWS", url, 200, ms, null, Map.of());
                    log.info("Ingested hourly forecast for {} (periods={})", gridId, written);
                    ok++;
                } catch (Exception e) {
                    logRepo.logEvent(runId, "NWS", url, null, null, e.getMessage(), Map.of());
                    fail++;
                    log.warn("Hourly ingest failed gridId={} url={} err={}", gridId, url, e.getMessage());
                }
            }

            logRepo.finishRun(runId, fail == 0, "ok=" + ok + " fail=" + fail + " points=" + gps.size());
            log.info("Finished ingestHourlyForecasts: ok={} fail={}", ok, fail);
        } catch (Exception outer) {
            logRepo.finishRun(runId, false, "fatal: " + outer.getMessage());
            throw outer;
        } finally {
            MDC.remove("runId");
        }
    }

    // -------------------------
    // JOB 3: Alerts (per each tracked point)
    // -------------------------
    /**
     * Loads active alerts for tracked points and stores them.
     */
    public void ingestAlerts() throws Exception {
        List<double[]> tracked = getTrackedPoints();
        log.info("Starting job: ingestAlerts (tracked points={})", tracked.size());
        UUID runId = logRepo.startRun("nws_alerts");
        MDC.put("runId", runId.toString());
        int ok = 0;
        int fail = 0;

        try {
            for (double[] pt : tracked) {
                double lat = pt[0];
                double lon = pt[1];
                String endpoint = "https://api.weather.gov/alerts/active?point=" + lat + "," + lon;

                try {
                    long t0 = System.currentTimeMillis();
                    JsonNode root = nws.activeAlertsForPoint(lat, lon);
                    int ms = (int) (System.currentTimeMillis() - t0);

                    JsonNode features = root.path("features");
                    if (!features.isArray())
                        throw new IllegalStateException("No features array");

                    int written = 0;
                    for (JsonNode feat : features) {
                        String id = feat.path("id").asText(null);
                        if (id == null)
                            continue;

                        JsonNode props = feat.path("properties");

                        String event = props.path("event").asText(null);
                        String severity = props.path("severity").asText(null);
                        String certainty = props.path("certainty").asText(null);
                        String urgency = props.path("urgency").asText(null);
                        String headline = props.path("headline").asText(null);
                        String description = props.path("description").asText(null);
                        String instruction = props.path("instruction").asText(null);

                        Instant effective = parseInstant(props.path("effective").asText(null));
                        Instant onset = parseInstant(props.path("onset").asText(null));
                        Instant expires = parseInstant(props.path("expires").asText(null));
                        Instant ends = parseInstant(props.path("ends").asText(null));

                        String status = props.path("status").asText(null);
                        String messageType = props.path("messageType").asText(null);
                        String areaDesc = props.path("areaDesc").asText(null);

                        JsonNode geomNode = feat.get("geometry"); // can be null
                        String geomGeoJson = (geomNode == null || geomNode.isNull()) ? null
                                : om.writeValueAsString(geomNode);

                        String raw = om.writeValueAsString(feat);

                        alertRepo.upsertAlert(
                                id, event, severity, certainty, urgency,
                                headline, description, instruction,
                                effective, onset, expires, ends,
                                status, messageType, areaDesc,
                                geomGeoJson, raw);
                        written++;
                    }
                    logRepo.logEvent(runId, "NWS", endpoint, 200, ms, null, Map.of());
                    log.info("Ingested alerts for point {},{} (count={})", lat, lon, written);
                    ok++;
                } catch (Exception e) {
                    logRepo.logEvent(runId, "NWS", endpoint, null, null, e.getMessage(), Map.of());
                    fail++;
                    log.warn("Alerts ingest failed for {},{}: {}", lat, lon, e.getMessage());
                }
            }

            logRepo.finishRun(runId, fail == 0, "ok=" + ok + " fail=" + fail);
            log.info("Finished ingestAlerts: ok={} fail={}", ok, fail);
        } catch (Exception outer) {
            logRepo.finishRun(runId, false, "fatal: " + outer.getMessage());
            throw outer;
        } finally {
            MDC.remove("runId");
        }
    }

    // -------------------------
    // Helpers
    // -------------------------
    /**
     * Parses an ISO-8601 timestamp string to an Instant.
     */
    private Instant parseInstant(String s) {
        if (s == null || s.isBlank())
            return null;
        try {
            return Instant.parse(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    // NWS hourly period temperature is usually numeric in "temperature" with
    // "temperatureUnit"
    /**
     * Converts an NWS temperature node to Celsius.
     */
    private Double toCelsius(JsonNode period) {
        if (period == null || period.isMissingNode())
            return null;
        JsonNode tNode = period.get("temperature");
        if (tNode == null || tNode.isNull())
            return null;

        double t = tNode.asDouble();
        String unit = period.path("temperatureUnit").asText("F");
        if ("C".equalsIgnoreCase(unit))
            return t;
        // assume F otherwise
        return (t - 32.0) * (5.0 / 9.0);
    }

    /**
     * Parses wind speed text and converts to meters per second.
     */
    private Double parseWindSpeedMps(String windText) {
        // examples: "5 mph", "10 to 15 mph", "15 kt"
        if (windText == null || windText.isBlank())
            return null;
        String lower = windText.toLowerCase();

        // find first number
        Double first = null;
        for (String tok : lower.replace("to", " ").split("\\s+")) {
            try {
                first = Double.parseDouble(tok);
                break;
            } catch (Exception ignored) {
            }
        }
        if (first == null)
            return null;

        if (lower.contains("kt") || lower.contains("kts")) {
            // knots -> m/s (1 kt = 0.514444 m/s)
            return first * 0.514444;
        }
        // mph -> m/s (1 mph = 0.44704 m/s)
        return first * 0.44704;
    }

    /**
     * Reads a probability node and converts it to 0..1.
     */
    private Double parsePrecipProb01(JsonNode probNode) {
        if (probNode == null || probNode.isMissingNode())
            return null;
        JsonNode v = probNode.get("value");
        if (v == null || v.isNull())
            return null;
        double percent = v.asDouble();
        return percent / 100.0;
    }

    /**
     * Reads a numeric "value" field from a JSON node.
     */
    private Double parseValueNode(JsonNode node) {
        if (node == null || node.isMissingNode())
            return null;
        JsonNode v = node.get("value");
        if (v == null || v.isNull())
            return null;
        return v.asDouble();
    }

    /**
     * Converts a compass direction (N, NE, E, etc.) to degrees.
     */
    private Double cardinalToDegrees(String dir) {
        if (dir == null || dir.isBlank())
            return null;
        dir = dir.trim().toUpperCase();

        // simple 8-wind compass
        return switch (dir) {
            case "N" -> 0.0;
            case "NE" -> 45.0;
            case "E" -> 90.0;
            case "SE" -> 135.0;
            case "S" -> 180.0;
            case "SW" -> 225.0;
            case "W" -> 270.0;
            case "NW" -> 315.0;
            default -> null;
        };
    }
}
