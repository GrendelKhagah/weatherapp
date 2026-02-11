/*
* Copyright 2025 Taylor Ketterling
* API Server for WeatherApp, a weather data ingestion and serving application.
* utalizes Javalin for HTTP server and provides various endpoints for weather data.
* uses Jackson for JSON processing and HikariCP for database connection pooling.
*/

package space.ketterling.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.ketterling.config.AppConfig;
import space.ketterling.db.ForecastRepo;
import space.ketterling.db.GridpointRepo;
import space.ketterling.db.TrackedPointRepo;
import space.ketterling.nws.NwsClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runs the HTTP API that the front end uses.
 * <p>
 * This class wires together routes, caching, and shared helpers so
 * requests can be served quickly and consistently.
 */
public class ApiServer {
    private static final Logger log = LoggerFactory.getLogger(ApiServer.class);

    private final AppConfig cfg;
    private final ObjectMapper om;
    private final HikariDataSource ds;
    private final ForecastRepo forecastRepo;
    private final NwsClient nws;
    private final Runnable gridpointRefreshTrigger;
    private final TrackedPointRepo trackedPointRepo;
    private final GridpointRepo gridpointRepo;
    private Javalin app;
    private final Map<String, CachedResponse> responseCache = new ConcurrentHashMap<>();

    /**
     * Creates an API server with default settings (no grid refresh hook, no NWS
     * client).
     */
    public ApiServer(AppConfig cfg, ObjectMapper om, HikariDataSource ds) {
        this(cfg, om, ds, null, null);
    }

    /**
     * Creates an API server with an optional gridpoint refresh hook.
     */
    public ApiServer(AppConfig cfg, ObjectMapper om, HikariDataSource ds, Runnable gridpointRefreshTrigger) {
        this(cfg, om, ds, gridpointRefreshTrigger, null);
    }

    /**
     * Creates an API server with optional grid refresh hook and NWS client.
     */
    public ApiServer(AppConfig cfg, ObjectMapper om, HikariDataSource ds, Runnable gridpointRefreshTrigger,
            NwsClient nws) {
        this.cfg = cfg;
        this.om = om;
        this.ds = ds;
        this.forecastRepo = new ForecastRepo(ds);
        this.gridpointRefreshTrigger = gridpointRefreshTrigger;
        this.nws = nws;
        this.trackedPointRepo = new TrackedPointRepo(ds);
        this.gridpointRepo = new GridpointRepo(ds);
    }

    /**
     * Starts the HTTP server and registers all routes.
     */
    public void start() {
        log.info("Starting API server on port {}", cfg.apiPort());
        app = Javalin.create(j -> {
            j.http.defaultContentType = "application/json";
            j.bundledPlugins.enableCors(cors -> cors.addRule(r -> r.anyHost()));
        });

        // Basic request logging + record start time for latency measurement
        app.before(ctx -> {
            ctx.attribute("startTime", System.currentTimeMillis());
            log.info("Incoming {} {} from {}", ctx.method(), ctx.path(), ctx.ip());
            ctx.header("Access-Control-Max-Age", "600");
        });

        app.options("/*", ctx -> {
            ctx.header("Access-Control-Max-Age", "600");
            ctx.status(200);
        });

        // After-handler to log response status and duration
        app.after(ctx -> {
            Long t0 = ctx.attribute("startTime");
            long ms = (t0 == null) ? -1 : (System.currentTimeMillis() - t0);
            log.info("Handled {} {} -> {} ({} ms)", ctx.method(), ctx.path(), ctx.status(), ms);
        });

        // Helpful JSON error instead of default HTML-ish errors
        app.exception(Exception.class, (e, ctx) -> {
            log.error("Unhandled error on {} {}", ctx.method(), ctx.path(), e);
            ctx.status(500).json(om.createObjectNode()
                    .put("error", "internal_error")
                    .put("message", e.getMessage() == null ? "Unknown error" : e.getMessage()));
        });

        ApiRoutesRoot.register(this);
        ApiRoutesMetrics.register(this);
        ApiRoutesTracked.register(this);
        ApiRoutesGeo.register(this);
        ApiRoutesForecast.register(this);
        ApiRoutesIngest.register(this);
        ApiRoutesMl.register(this);
        ApiRoutesLayer.register(this);
        ApiRoutesHistory.register(this);
        ApiRoutesPointSummary.register(this);

        app.start(cfg.apiPort());
    }

    /**
     * Stops the HTTP server.
     */
    public void stop() {
        if (app != null)
            app.stop();
        log.info("API server stopped");
    }

    /**
     * @return the Javalin server instance
     */
    Javalin app() {
        return app;
    }

    /**
     * @return the loaded application configuration
     */
    AppConfig cfg() {
        return cfg;
    }

    /**
     * @return Jackson JSON mapper used across routes
     */
    ObjectMapper om() {
        return om;
    }

    /**
     * @return database connection pool
     */
    HikariDataSource ds() {
        return ds;
    }

    /**
     * @return forecast repository for DB writes
     */
    ForecastRepo forecastRepo() {
        return forecastRepo;
    }

    /**
     * @return NWS HTTP client (may be null)
     */
    NwsClient nws() {
        return nws;
    }

    /**
     * @return tracked point repository
     */
    TrackedPointRepo trackedPointRepo() {
        return trackedPointRepo;
    }

    /**
     * @return gridpoint repository
     */
    GridpointRepo gridpointRepo() {
        return gridpointRepo;
    }

    /**
     * @return optional hook that refreshes gridpoints
     */
    Runnable gridpointRefreshTrigger() {
        return gridpointRefreshTrigger;
    }

    // --------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------
    /**
     * Simple cached response wrapper used for in-memory caching.
     */
    static class CachedResponse {
        final String body;
        final long expiresAt;
        final String etag;
        final int maxAgeSeconds;
        final int staleSeconds;

        /**
         * Stores a cached response and cache timing metadata.
         */
        CachedResponse(String body, long expiresAt, String etag, int maxAgeSeconds, int staleSeconds) {
            this.body = body;
            this.expiresAt = expiresAt;
            this.etag = etag;
            this.maxAgeSeconds = maxAgeSeconds;
            this.staleSeconds = staleSeconds;
        }
    }

    /**
     * Tries to serve a cached response by key.
     *
     * @return true if a cached response was sent
     */
    boolean serveCached(Context ctx, String key) {
        CachedResponse cached = responseCache.get(key);
        if (cached == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (cached.expiresAt <= now) {
            responseCache.remove(key);
            return false;
        }

        String inm = ctx.header("If-None-Match");
        if (cached.etag != null && cached.etag.equals(inm)) {
            applyCacheHeaders(ctx, cached.maxAgeSeconds, cached.staleSeconds);
            ctx.header("ETag", cached.etag);
            ctx.status(304);
            return true;
        }

        applyCacheHeaders(ctx, cached.maxAgeSeconds, cached.staleSeconds);
        ctx.header("ETag", cached.etag);
        ctx.contentType("application/json");
        ctx.result(cached.body);
        return true;
    }

    /**
     * Saves a JSON response in cache and sends it to the client.
     */
    void cacheAndRespond(Context ctx, String key, JsonNode node, int maxAgeSeconds, int staleSeconds)
            throws Exception {
        String body = om.writeValueAsString(node);
        String etag = "\"" + Integer.toHexString(body.hashCode()) + "\"";
        long expiresAt = System.currentTimeMillis() + (maxAgeSeconds * 1000L);
        responseCache.put(key, new CachedResponse(body, expiresAt, etag, maxAgeSeconds, staleSeconds));
        applyCacheHeaders(ctx, maxAgeSeconds, staleSeconds);
        ctx.header("ETag", etag);
        ctx.contentType("application/json");
        ctx.result(body);
    }

    /**
     * Adds Cache-Control headers to the response.
     */
    void applyCacheHeaders(Context ctx, int maxAgeSeconds, int staleSeconds) {
        if (maxAgeSeconds <= 0) {
            return;
        }
        StringBuilder sb = new StringBuilder("public, max-age=").append(maxAgeSeconds);
        if (staleSeconds > 0) {
            sb.append(", stale-while-revalidate=").append(staleSeconds);
        }
        ctx.header("Cache-Control", sb.toString());
    }

    /**
     * Builds an empty GeoJSON FeatureCollection.
     */
    ObjectNode featureCollection() {
        ObjectNode fc = om.createObjectNode();
        fc.put("type", "FeatureCollection");
        fc.set("features", om.createArrayNode());
        return fc;
    }

    /**
     * Rounds a number to the requested number of decimal places.
     */
    static double roundTo(double value, int places) {
        if (places < 0)
            return value;
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    /**
     * Formats a bounding box into a small cache key string.
     */
    static String bboxKey(double[] bbox) {
        if (bbox == null || bbox.length != 4)
            return "invalid";
        return String.format("%.3f,%.3f,%.3f,%.3f",
                roundTo(bbox[0], 3),
                roundTo(bbox[1], 3),
                roundTo(bbox[2], 3),
                roundTo(bbox[3], 3));
    }

    /**
     * Parses "minLon,minLat,maxLon,maxLat" into a double array.
     */
    double[] parseBbox(String bbox) {
        if (bbox == null || bbox.isBlank())
            return null;
        // bbox=minLon,minLat,maxLon,maxLat
        String[] parts = bbox.split(",");
        if (parts.length != 4)
            return null;
        try {
            return new double[] {
                    Double.parseDouble(parts[0].trim()),
                    Double.parseDouble(parts[1].trim()),
                    Double.parseDouble(parts[2].trim()),
                    Double.parseDouble(parts[3].trim())
            };
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Checks if a latitude/longitude is within normal world bounds.
     */
    static boolean isLatLonValid(Double lat, Double lon) {
        if (lat == null || lon == null)
            return false;
        return lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0;
    }

    /**
     * Writes a value into JSON, handling nulls and numbers safely.
     */
    void putNullable(ObjectNode obj, String key, Object value) {
        if (value == null)
            obj.putNull(key);
        else if (value instanceof Number n)
            obj.put(key, n.doubleValue());
        else
            obj.put(key, value.toString());
    }

    /**
     * Parses an int with bounds and a default fallback.
     */
    static Integer parseInt(String s, Integer def, int min, int max) {
        if (s == null || s.isBlank())
            return def;
        try {
            int v = Integer.parseInt(s.trim());
            if (v < min)
                v = min;
            if (v > max)
                v = max;
            return v;
        } catch (Exception e) {
            return def;
        }
    }

    /**
     * Parses a double with a default fallback.
     */
    static Double parseDouble(String s, Double def) {
        if (s == null || s.isBlank())
            return def;
        try {
            return Double.parseDouble(s.trim());
        } catch (Exception e) {
            return def;
        }
    }

    /**
     * Converts an NWS temperature node to Celsius.
     */
    static Double toCelsius(JsonNode period) {
        if (period == null || period.isMissingNode())
            return null;
        JsonNode tNode = period.get("temperature");
        if (tNode == null || tNode.isNull())
            return null;

        double t = tNode.asDouble();
        String unit = period.path("temperatureUnit").asText("F");
        if ("C".equalsIgnoreCase(unit))
            return t;
        return (t - 32.0) * (5.0 / 9.0);
    }

    /**
     * Reads wind speed text (mph/kts) and converts to meters per second.
     */
    static Double parseWindSpeedMps(String windText) {
        if (windText == null || windText.isBlank())
            return null;
        String lower = windText.toLowerCase();
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
            return first * 0.514444;
        }
        return first * 0.44704;
    }

    /**
     * Converts a probability node to a 0..1 value.
     */
    static Double parsePrecipProb01(JsonNode probNode) {
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
    static Double parseValueNode(JsonNode node) {
        if (node == null || node.isMissingNode())
            return null;
        JsonNode v = node.get("value");
        if (v == null || v.isNull())
            return null;
        return v.asDouble();
    }

    /**
     * Converts a cardinal wind direction to degrees.
     */
    static Double cardinalToDegrees(String dir) {
        if (dir == null || dir.isBlank())
            return null;
        dir = dir.trim().toUpperCase();

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

    /**
     * Reads hourly forecast rows from the database for a gridpoint.
     */
    ArrayNode fetchHourlyFromDb(String gridId, int limit) throws Exception {
        String sql = """
                SELECT start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg,
                       precip_prob, relative_humidity, short_forecast
                FROM v_latest_hourly_forecast
                WHERE grid_id = ?
                AND end_time >= now()
                ORDER BY start_time ASC
                LIMIT ?
                """;

        ArrayNode arr = om.createArrayNode();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, gridId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("start_time", rs.getString("start_time"));
                    row.put("end_time", rs.getString("end_time"));
                    putNullable(row, "temperature_c", rs.getObject("temperature_c"));
                    putNullable(row, "wind_speed_mps", rs.getObject("wind_speed_mps"));
                    putNullable(row, "wind_gust_mps", rs.getObject("wind_gust_mps"));
                    putNullable(row, "wind_dir_deg", rs.getObject("wind_dir_deg"));
                    putNullable(row, "precip_prob", rs.getObject("precip_prob"));
                    putNullable(row, "relative_humidity", rs.getObject("relative_humidity"));
                    row.put("short_forecast", rs.getString("short_forecast"));
                    arr.add(row);
                }
            }
        }
        return arr;
    }

    /**
     * Fetches hourly forecast from NWS, stores it in DB, and returns JSON.
     */
    ObjectNode fetchAndStoreHourlyFromNws(String gridId, String forecastHourlyUrl, ObjectNode out)
            throws Exception {
        JsonNode hourlyRoot = nws.forecastHourly(forecastHourlyUrl);
        JsonNode props = hourlyRoot.path("properties");
        JsonNode periods = props.path("periods");
        if (!periods.isArray() || periods.size() == 0) {
            throw new IllegalStateException("No hourly periods");
        }

        Instant issuedAt = parseInstant(props.path("updated").asText(null));
        ArrayNode list = om.createArrayNode();
        int count = periods.size();
        for (int i = 0; i < count; i++) {
            JsonNode p = periods.get(i);
            ObjectNode row = om.createObjectNode();
            row.put("start_time", p.path("startTime").asText(null));
            row.put("end_time", p.path("endTime").asText(null));
            Double tempC = toCelsius(p);
            Double windSpeed = parseWindSpeedMps(p.path("windSpeed").asText(null));
            Double windGust = parseWindSpeedMps(p.path("windGust").asText(null));
            Double windDir = cardinalToDegrees(p.path("windDirection").asText(null));
            Double precip = parsePrecipProb01(p.path("probabilityOfPrecipitation"));
            Double rh = parseValueNode(p.path("relativeHumidity"));
            row.put("short_forecast", p.path("shortForecast").asText(null));
            putNullable(row, "temperature_c", tempC);
            putNullable(row, "wind_speed_mps", windSpeed);
            putNullable(row, "wind_gust_mps", windGust);
            putNullable(row, "wind_dir_deg", windDir);
            putNullable(row, "precip_prob", precip);
            putNullable(row, "relative_humidity", rh);
            list.add(row);

            Instant start = parseInstant(p.path("startTime").asText(null));
            Instant end = parseInstant(p.path("endTime").asText(null));
            if (start != null && end != null) {
                forecastRepo.upsertHourly(
                        gridId,
                        start,
                        end,
                        tempC,
                        windSpeed,
                        windGust,
                        windDir,
                        precip,
                        rh,
                        p.path("shortForecast").asText(null),
                        issuedAt,
                        om.writeValueAsString(p));
            }
        }

        out.put("source", "nws");
        out.set("periods", list);
        out.set("hourly", list.get(0));
        return out;
    }

    /**
     * Parses an ISO-8601 timestamp into an Instant.
     */
    Instant parseInstant(String s) {
        if (s == null || s.isBlank())
            return null;
        try {
            return Instant.parse(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Returns the first non-empty string from two options.
     */
    static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank())
            return a;
        if (b != null && !b.isBlank())
            return b;
        return null;
    }
}
