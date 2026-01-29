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
import space.ketterling.metrics.ExternalApiMetrics;
import space.ketterling.nws.NwsClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ApiServer {
    private static final Logger log = LoggerFactory.getLogger(ApiServer.class);

    private final AppConfig cfg;
    private final ObjectMapper om;
    private final HikariDataSource ds;
    private final ForecastRepo forecastRepo;
    private final NwsClient nws;
    private final Runnable gridpointRefreshTrigger;
    private Javalin app;
    private final Map<String, CachedResponse> responseCache = new ConcurrentHashMap<>();

    public ApiServer(AppConfig cfg, ObjectMapper om, HikariDataSource ds) {
        this(cfg, om, ds, null, null);
    }

    public ApiServer(AppConfig cfg, ObjectMapper om, HikariDataSource ds, Runnable gridpointRefreshTrigger) {
        this(cfg, om, ds, gridpointRefreshTrigger, null);
    }

    public ApiServer(AppConfig cfg, ObjectMapper om, HikariDataSource ds, Runnable gridpointRefreshTrigger,
            NwsClient nws) {
        this.cfg = cfg;
        this.om = om;
        this.ds = ds;
        this.forecastRepo = new ForecastRepo(ds);
        this.gridpointRefreshTrigger = gridpointRefreshTrigger;
        this.nws = nws;
    }

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

        TrackedPointRepo trackedPointRepo = new TrackedPointRepo(ds);
        GridpointRepo gridpointRepo = new GridpointRepo(ds);

        // --------------------------------------------------------------------
        // Root + Health
        // --------------------------------------------------------------------
        app.get("/", ctx -> ctx.json(Map.of(
                "service", "weatherapp",
                "status", "ok",
                "endpoints", new String[] {
                        "GET /health",
                        "GET /api/metrics/summary",
                        "GET /api/metrics/external",
                        "GET /api/gridpoints (GeoJSON)",
                        "GET /api/gridpoints/list",
                        "GET /api/alerts (GeoJSON)",
                        "GET /api/alerts/active",
                        "GET /api/forecast/hourly?gridId=LOX:149,46&limit=96",
                        "GET /api/forecast/hourly/point?lat=34.05&lon=-118.4",
                        "GET /api/stations/near?lat=34.05&lon=-118.4",
                        "GET /api/point/summary?lat=34.05&lon=-118.4",
                        "GET /api/tracked-points",
                        "GET /api/ingest/runs",
                        "GET /api/ingest/events?runId=<uuid>",
                        "GET /api/ml/runs",
                        "GET /api/ml/predictions/latest?gridId=LOX:149,46",
                        "GET /api/ml/weather/latest?sourceType=point&lat=34.05&lon=-118.4"
                })));

        app.get("/health", ctx -> {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("status", "ok");
            out.put("time", OffsetDateTime.now().toString());

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement("SELECT 1");
                    ResultSet rs = ps.executeQuery()) {
                out.put("db", rs.next() ? "ok" : "unknown");
            } catch (Exception e) {
                out.put("db", "fail");
                out.put("db_error", e.getMessage());
                ctx.status(503);
            }

            ctx.json(out);
        });

        // --------------------------------------------------------------------
        // Summary metrics: quick proof ingest is landing data
        // --------------------------------------------------------------------
        app.get("/api/metrics/summary", ctx -> {
            String cacheKey = "metrics:summary";
            if (serveCached(ctx, cacheKey)) {
                return;
            }
            ObjectNode out = om.createObjectNode();

            // Counts + latest ingested timestamps
            String sql = """
                            SELECT
                                (SELECT COUNT(*) FROM geo_gridpoint) AS gridpoints,
                                (SELECT COUNT(*) FROM nws_forecast_hourly) AS hourly_rows,
                                (SELECT COUNT(*) FROM nws_alert) AS alert_rows,
                                (SELECT COUNT(*) FROM noaa_station) AS noaa_stations,
                                (SELECT COUNT(DISTINCT station_id) FROM noaa_daily_summary) AS noaa_stations_with_data,
                                (SELECT COUNT(*) FROM noaa_daily_summary) AS noaa_daily_rows,
                                (SELECT COUNT(*) FROM tracked_point) AS tracked_points,
                                (SELECT MAX(ingested_at) FROM nws_forecast_hourly) AS latest_hourly_ingest,
                                (SELECT MAX(ingested_at) FROM nws_alert) AS latest_alert_ingest
                    """;

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    out.put("gridpoints", rs.getLong("gridpoints"));
                    out.put("hourly_rows", rs.getLong("hourly_rows"));
                    out.put("alert_rows", rs.getLong("alert_rows"));
                    out.put("noaa_stations", rs.getLong("noaa_stations"));
                    out.put("noaa_stations_with_data", rs.getLong("noaa_stations_with_data"));
                    out.put("noaa_daily_rows", rs.getLong("noaa_daily_rows"));
                    out.put("tracked_points", rs.getLong("tracked_points"));
                    out.put("latest_hourly_ingest", String.valueOf(rs.getObject("latest_hourly_ingest")));
                    out.put("latest_alert_ingest", String.valueOf(rs.getObject("latest_alert_ingest")));
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, out, 15, 30);
            } catch (Exception e) {
                ctx.json(out);
            }
        });

        // --------------------------------------------------------------------
        // External API metrics (rolling 60 minutes)
        // --------------------------------------------------------------------
        app.get("/api/metrics/external", ctx -> {
            ObjectNode out = om.createObjectNode();
            out.put("window_minutes", ExternalApiMetrics.windowMinutes());
            ArrayNode services = out.putArray("services");

            var snapshots = ExternalApiMetrics.snapshot();
            for (var e : snapshots.entrySet()) {
                var snap = e.getValue();
                ObjectNode row = om.createObjectNode();
                row.put("service", e.getKey());
                row.put("calls_last_hour", snap.callsLastHour);
                row.put("failures_last_hour", snap.failuresLastHour);
                row.put("failure_pct", snap.failurePct);
                row.put("status", snap.status);
                services.add(row);
            }

            ctx.json(out);
        });

        // --------------------------------------------------------------------
        // Tracked points (POI) management
        // --------------------------------------------------------------------
        app.get("/api/tracked-points", ctx -> {
            ArrayNode arr = om.createArrayNode();
            try {
                for (var p : trackedPointRepo.list()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("id", p.id());
                    row.put("name", p.name());
                    row.put("lat", p.lat());
                    row.put("lon", p.lon());
                    row.put("created_at", p.createdAt());
                    arr.add(row);
                }
            } catch (Exception e) {
                log.warn("Failed to list tracked points: {}", e.getMessage());
                ctx.status(500).json(om.createObjectNode().put("error", "tracked_points_list_failed"));
                return;
            }
            ctx.json(arr);
        });

        app.post("/api/tracked-points", ctx -> {
            String name = ctx.queryParam("name");
            Double lat = null;
            Double lon = null;
            try {
                lat = Double.parseDouble(ctx.queryParam("lat"));
                lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }
            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }
            if (name == null || name.isBlank()) {
                name = "Tracked Point";
            }

            try {
                long id = trackedPointRepo.upsert(name, lat, lon);
                ctx.json(om.createObjectNode().put("id", id).put("status", "ok"));
            } catch (Exception e) {
                log.warn("Failed to upsert tracked point: {}", e.getMessage());
                ctx.status(500).json(om.createObjectNode().put("error", "tracked_point_upsert_failed"));
            }
        });

        app.delete("/api/tracked-points", ctx -> {
            String idStr = ctx.queryParam("id");
            Integer deleted = null;
            try {
                if (idStr != null && !idStr.isBlank()) {
                    deleted = trackedPointRepo.deleteById(Long.parseLong(idStr));
                } else {
                    Double lat = Double.parseDouble(ctx.queryParam("lat"));
                    Double lon = Double.parseDouble(ctx.queryParam("lon"));
                    deleted = trackedPointRepo.deleteByLatLon(lat, lon);
                }
            } catch (Exception e) {
                ctx.status(400).json(om.createObjectNode().put("error", "invalid id or lat/lon"));
                return;
            }

            ctx.json(om.createObjectNode().put("deleted", deleted == null ? 0 : deleted));
        });

        app.post("/api/tracked-points/refresh", ctx -> {
            if (gridpointRefreshTrigger == null) {
                ctx.status(503).json(om.createObjectNode().put("error", "refresh_not_available"));
                return;
            }

            new Thread(() -> {
                try {
                    gridpointRefreshTrigger.run();
                } catch (Exception e) {
                    log.warn("Manual gridpoint refresh failed: {}", e.getMessage());
                }
            }, "api-gridpoint-refresh").start();

            ctx.json(om.createObjectNode().put("status", "started"));
        });

        // --------------------------------------------------------------------
        // MapLibre layers (GeoJSON)
        // --------------------------------------------------------------------
        app.get("/api/gridpoints", ctx -> {
            log.info("GET /api/gridpoints bbox={}", ctx.queryParam("bbox"));
            double[] bbox = parseBbox(ctx.queryParam("bbox"));
            String cacheKey = "gridpoints:" + (bbox == null ? "all" : bboxKey(bbox));
            if (serveCached(ctx, cacheKey)) {
                return;
            }
            ObjectNode fc = featureCollection();

            String sql = "SELECT grid_id, office, grid_x, grid_y, ST_AsGeoJSON(geom) AS geom " +
                    "FROM geo_gridpoint " +
                    (bbox != null ? "WHERE ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?, 4326)) " : "") +
                    "ORDER BY grid_id";

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {

                if (bbox != null) {
                    ps.setDouble(1, bbox[0]); // minLon
                    ps.setDouble(2, bbox[1]); // minLat
                    ps.setDouble(3, bbox[2]); // maxLon
                    ps.setDouble(4, bbox[3]); // maxLat
                }

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("grid_id", rs.getString("grid_id"));
                        props.put("office", rs.getString("office"));
                        props.put("grid_x", rs.getInt("grid_x"));
                        props.put("grid_y", rs.getInt("grid_y"));
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, fc, 30, 120);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/alerts", ctx -> {
            log.info("GET /api/alerts bbox={}", ctx.queryParam("bbox"));
            double[] bbox = parseBbox(ctx.queryParam("bbox"));
            String cacheKey = "alerts:" + (bbox == null ? "all" : bboxKey(bbox));
            if (serveCached(ctx, cacheKey)) {
                return;
            }
            ObjectNode fc = featureCollection();

            String sql = "SELECT alert_id, event, severity, urgency, headline, expires, ST_AsGeoJSON(geom) AS geom " +
                    "FROM v_active_alerts " +
                    "WHERE geom IS NOT NULL " +
                    (bbox != null ? "AND ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?, 4326)) " : "") +
                    "ORDER BY expires NULLS LAST";

            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {

                if (bbox != null) {
                    ps.setDouble(1, bbox[0]);
                    ps.setDouble(2, bbox[1]);
                    ps.setDouble(3, bbox[2]);
                    ps.setDouble(4, bbox[3]);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("alert_id", rs.getString("alert_id"));
                        props.put("event", rs.getString("event"));
                        props.put("severity", rs.getString("severity"));
                        props.put("urgency", rs.getString("urgency"));
                        props.put("headline", rs.getString("headline"));
                        props.put("expires", rs.getString("expires"));
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, fc, 30, 120);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/stations/near", ctx -> {
            Double lat = null;
            Double lon = null;
            try {
                lat = Double.parseDouble(ctx.queryParam("lat"));
                lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }
            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }
            int limit = parseInt(ctx.queryParam("limit"), 10, 1, 50);

            String cacheKey = String.format("stations:%.4f,%.4f:%d", roundTo(lat, 4), roundTo(lon, 4), limit);
            if (serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode fc = featureCollection();
            String sql = """
                    WITH nearest AS (
                        SELECT station_id, name, geom,
                               ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS dist_m
                        FROM noaa_station
                        WHERE geom IS NOT NULL
                        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)
                        LIMIT ?
                    ), daily AS (
                        SELECT n.*, d.date AS latest_date, d.tmax_c, d.tmin_c, d.prcp_mm,
                               cov.rows_total, cov.rows_tmax, cov.rows_tmin, cov.rows_prcp,
                               cov.first_date, cov.last_date
                        FROM nearest n
                        LEFT JOIN LATERAL (
                            SELECT date, tmax_c, tmin_c, prcp_mm
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                            ORDER BY date DESC
                            LIMIT 1
                        ) d ON true
                        LEFT JOIN LATERAL (
                            SELECT COUNT(*) AS rows_total,
                                   COUNT(tmax_c) AS rows_tmax,
                                   COUNT(tmin_c) AS rows_tmin,
                                   COUNT(prcp_mm) AS rows_prcp,
                                   MIN(date) AS first_date,
                                   MAX(date) AS last_date
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                        ) cov ON true
                    )
                    SELECT station_id, name, dist_m, latest_date, tmax_c, tmin_c, prcp_mm,
                           rows_total, rows_tmax, rows_tmin, rows_prcp, first_date, last_date,
                           ST_AsGeoJSON(geom) AS geom
                    FROM daily
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                ps.setDouble(3, lon);
                ps.setDouble(4, lat);
                ps.setInt(5, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("station_id", rs.getString("station_id"));
                        props.put("name", rs.getString("name"));
                        props.put("dist_km", rs.getDouble("dist_m") / 1000.0);
                        props.put("latest_date", String.valueOf(rs.getObject("latest_date")));
                        putNullable(props, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(props, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(props, "prcp_mm", rs.getObject("prcp_mm"));
                        putNullable(props, "rows_total", rs.getObject("rows_total"));
                        putNullable(props, "rows_tmax", rs.getObject("rows_tmax"));
                        putNullable(props, "rows_tmin", rs.getObject("rows_tmin"));
                        putNullable(props, "rows_prcp", rs.getObject("rows_prcp"));
                        putNullable(props, "first_date", rs.getObject("first_date"));
                        putNullable(props, "last_date", rs.getObject("last_date"));
                        if (rs.getObject("tmax_c") != null && rs.getObject("tmin_c") != null) {
                            double tmean = (rs.getDouble("tmax_c") + rs.getDouble("tmin_c")) / 2.0;
                            props.put("tmean_c", tmean);
                        }
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, fc, 300, 600);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        app.get("/api/stations/all", ctx -> {
            double[] bbox = parseBbox(ctx.queryParam("bbox"));
            if (bbox == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "bbox is required"));
                return;
            }
            int limit = parseInt(ctx.queryParam("limit"), 5000, 1, 20000);
            String withDataParam = ctx.queryParam("withData");
            boolean withData = withDataParam != null && ("1".equals(withDataParam)
                    || "true".equalsIgnoreCase(withDataParam)
                    || "yes".equalsIgnoreCase(withDataParam));

            String cacheKey = "stations-all:" + bboxKey(bbox) + ":" + limit + ":" + (withData ? "data" : "all");
            if (serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode fc = featureCollection();
            String sql = """
                        SELECT s.station_id, s.name, d.date AS latest_date, d.tmax_c, d.tmin_c, d.prcp_mm,
                               ST_AsGeoJSON(s.geom) AS geom
                        FROM noaa_station s
                        LEFT JOIN LATERAL (
                            SELECT date, tmax_c, tmin_c, prcp_mm
                            FROM noaa_daily_summary
                            WHERE station_id = s.station_id
                            ORDER BY date DESC
                            LIMIT 1
                        ) d ON true
                        WHERE s.geom IS NOT NULL
                          AND ST_Intersects(s.geom, ST_MakeEnvelope(?, ?, ?, ?, 4326))
                          AND (? = false OR d.date IS NOT NULL)
                        ORDER BY (d.date IS NULL) ASC, d.date DESC
                        LIMIT ?
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setDouble(1, bbox[0]); // minLon
                ps.setDouble(2, bbox[1]); // minLat
                ps.setDouble(3, bbox[2]); // maxLon
                ps.setDouble(4, bbox[3]); // maxLat
                ps.setBoolean(5, withData);
                ps.setInt(6, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");

                        ObjectNode props = om.createObjectNode();
                        props.put("station_id", rs.getString("station_id"));
                        props.put("name", rs.getString("name"));
                        props.put("latest_date", String.valueOf(rs.getObject("latest_date")));
                        putNullable(props, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(props, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(props, "prcp_mm", rs.getObject("prcp_mm"));
                        if (rs.getObject("tmax_c") != null && rs.getObject("tmin_c") != null) {
                            double tmean = (rs.getDouble("tmax_c") + rs.getDouble("tmin_c")) / 2.0;
                            props.put("tmean_c", tmean);
                        }
                        feat.set("properties", props);

                        String geomJson = rs.getString("geom");
                        feat.set("geometry", geomJson == null ? om.nullNode() : om.readTree(geomJson));

                        feats.add(feat);
                    }
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, fc, 300, 600);
            } catch (Exception e) {
                ctx.json(fc);
            }
        });

        // --------------------------------------------------------------------
        // Non-GeoJSON list endpoints
        // --------------------------------------------------------------------
        app.get("/api/gridpoints/list", ctx -> {
            String sql = """
                        SELECT grid_id, office, grid_x, grid_y,
                               ST_Y(geom) AS lat, ST_X(geom) AS lon,
                               forecast_hourly_url, last_refreshed_at
                        FROM geo_gridpoint
                        ORDER BY grid_id
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("grid_id", rs.getString("grid_id"));
                    row.put("office", rs.getString("office"));
                    row.put("grid_x", rs.getInt("grid_x"));
                    row.put("grid_y", rs.getInt("grid_y"));
                    row.put("lat", rs.getDouble("lat"));
                    row.put("lon", rs.getDouble("lon"));
                    row.put("forecast_hourly_url", rs.getString("forecast_hourly_url"));
                    row.put("last_refreshed_at", String.valueOf(rs.getObject("last_refreshed_at")));
                    arr.add(row);
                }
            }

            ctx.json(arr);
        });

        app.get("/api/alerts/active", ctx -> {
            String sql = """
                        SELECT alert_id, event, severity, certainty, urgency,
                               headline, effective, onset, expires, ends,
                               area_desc, ingested_at
                        FROM v_active_alerts
                        ORDER BY expires NULLS LAST, ingested_at DESC
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("alert_id", rs.getString("alert_id"));
                    row.put("event", rs.getString("event"));
                    row.put("severity", rs.getString("severity"));
                    row.put("certainty", rs.getString("certainty"));
                    row.put("urgency", rs.getString("urgency"));
                    row.put("headline", rs.getString("headline"));
                    row.put("effective", String.valueOf(rs.getObject("effective")));
                    row.put("onset", String.valueOf(rs.getObject("onset")));
                    row.put("expires", String.valueOf(rs.getObject("expires")));
                    row.put("ends", String.valueOf(rs.getObject("ends")));
                    row.put("area_desc", rs.getString("area_desc"));
                    row.put("ingested_at", String.valueOf(rs.getObject("ingested_at")));
                    arr.add(row);
                }
            }

            ctx.json(arr);
        });

        // --------------------------------------------------------------------
        // Forecast hourly (supports gridId or grid_id, limit or hours)
        // --------------------------------------------------------------------
        app.get("/api/forecast/hourly", ctx -> {
            String gridId = firstNonBlank(ctx.queryParam("gridId"), ctx.queryParam("grid_id"));
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId (or grid_id) is required"));
                return;
            }

            Integer limit = parseInt(ctx.queryParam("limit"), 96, 1, 240);
            Integer hours = parseInt(ctx.queryParam("hours"), null, 1, 168);

            String startIso = ctx.queryParam("start");
            String endIso = ctx.queryParam("end");

            StringBuilder sql = new StringBuilder("""
                        SELECT start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg,
                               precip_prob, relative_humidity, short_forecast, issued_at, ingested_at
                        FROM v_latest_hourly_forecast
                        WHERE grid_id=?
                    """);

            if (hours != null) {
                sql.append(" AND start_time >= now() AND start_time < now() + (? || ' hours')::interval ");
            }
            if (startIso != null && !startIso.isBlank()) {
                sql.append(" AND start_time >= ?::timestamptz ");
            }
            if (endIso != null && !endIso.isBlank()) {
                sql.append(" AND start_time < ?::timestamptz ");
            }

            sql.append(" ORDER BY start_time ");

            if (hours == null) {
                sql.append(" LIMIT ? ");
            }

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
                int i = 1;
                ps.setString(i++, gridId);

                if (hours != null) {
                    ps.setInt(i++, hours);
                }
                if (startIso != null && !startIso.isBlank()) {
                    ps.setString(i++, startIso);
                }
                if (endIso != null && !endIso.isBlank()) {
                    ps.setString(i++, endIso);
                }
                if (hours == null) {
                    ps.setInt(i, limit);
                }

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
                        row.put("issued_at", String.valueOf(rs.getObject("issued_at")));
                        row.put("ingested_at", String.valueOf(rs.getObject("ingested_at")));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        // --------------------------------------------------------------------
        // Forecast daily (derived from hourly)
        // --------------------------------------------------------------------
        app.get("/api/forecast/daily", ctx -> {
            String gridId = firstNonBlank(ctx.queryParam("gridId"), ctx.queryParam("grid_id"));
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId (or grid_id) is required"));
                return;
            }

            int days = parseInt(ctx.queryParam("days"), 10, 1, 14);

            String sql = """
                        SELECT date_trunc('day', start_time) AS day,
                               MIN(temperature_c) AS tmin_c,
                               MAX(temperature_c) AS tmax_c,
                               AVG(precip_prob) AS precip_prob
                        FROM v_latest_hourly_forecast
                        WHERE grid_id = ?
                          AND start_time >= now()
                          AND start_time < now() + (? || ' days')::interval
                        GROUP BY 1
                        ORDER BY 1
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, gridId);
                ps.setInt(2, days);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("day", String.valueOf(rs.getObject("day")));
                        putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(row, "precip_prob", rs.getObject("precip_prob"));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        // --------------------------------------------------------------------
        // Live hourly forecast for a lat/lon (NWS points -> forecastHourly)
        // --------------------------------------------------------------------
        app.get("/api/forecast/hourly/point", ctx -> {
            if (nws == null) {
                ctx.status(503).json(om.createObjectNode().put("error", "NWS client unavailable"));
                return;
            }

            Double lat = parseDouble(ctx.queryParam("lat"), null);
            Double lon = parseDouble(ctx.queryParam("lon"), null);
            Integer limit = parseInt(ctx.queryParam("limit"), 24, 1, 168);
            String mode = ctx.queryParam("mode");
            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }

            String cacheKey = String.format("hourlyPoint:%.4f,%.4f:%d:%s", roundTo(lat, 4), roundTo(lon, 4),
                    limit, mode == null ? "" : mode.toLowerCase());
            if (serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode nearest = null;
            Double nearestDistM = null;
            String nearestHourlyUrl = null;
            String nearestOffice = null;
            String nearestGridId = null;
            Integer nearestGridX = null;
            Integer nearestGridY = null;

            String nearestSql = """
                    SELECT grid_id, office, grid_x, grid_y, forecast_hourly_url,
                           ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS dist_m
                    FROM geo_gridpoint
                    WHERE geom IS NOT NULL
                    ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)
                    LIMIT 1
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(nearestSql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                ps.setDouble(3, lon);
                ps.setDouble(4, lat);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        nearestGridId = rs.getString("grid_id");
                        nearestOffice = rs.getString("office");
                        nearestGridX = rs.getInt("grid_x");
                        nearestGridY = rs.getInt("grid_y");
                        nearestHourlyUrl = rs.getString("forecast_hourly_url");
                        nearestDistM = rs.getDouble("dist_m");
                    }
                }
            }

            double maxDistM = 274.32; // 900 ft

            if (nearestGridId != null && nearestDistM != null && nearestDistM <= maxDistM) {
                ObjectNode out = om.createObjectNode();
                out.put("lat", lat);
                out.put("lon", lon);
                out.put("grid_id", nearestGridId);
                out.put("office", nearestOffice);
                out.put("source", "db");

                ArrayNode list = fetchHourlyFromDb(nearestGridId, limit);
                if (list != null && list.size() > 0) {
                    if (mode != null && mode.equalsIgnoreCase("list")) {
                        out.set("periods", list);
                    } else {
                        out.set("hourly", list.get(0));
                    }
                    try {
                        cacheAndRespond(ctx, cacheKey, out, 300, 600);
                    } catch (Exception e) {
                        ctx.json(out);
                    }
                    return;
                }

                if (nearestHourlyUrl != null && !nearestHourlyUrl.isBlank()) {
                    ObjectNode outNws = fetchAndStoreHourlyFromNws(nearestGridId, nearestHourlyUrl, out);
                    try {
                        cacheAndRespond(ctx, cacheKey, outNws, 300, 600);
                    } catch (Exception e) {
                        ctx.json(outNws);
                    }
                    return;
                }
            }

            JsonNode point = nws.points(lat, lon);
            JsonNode props = point.path("properties");
            String gridId = props.path("gridId").asText(null);
            int gridX = props.path("gridX").asInt();
            int gridY = props.path("gridY").asInt();
            String gridDataUrl = props.path("forecastGridData").asText(null);
            String forecastHourlyUrl = props.path("forecastHourly").asText(null);
            if (forecastHourlyUrl == null || forecastHourlyUrl.isBlank() || gridId == null) {
                ctx.status(503).json(om.createObjectNode().put("error", "Missing forecastHourly URL"));
                return;
            }

            String fullGridId = gridId + ":" + gridX + "," + gridY;
            GridpointRepo.GridpointDetail existing = gridpointRepo.getGridpointById(fullGridId);

            ObjectNode out = om.createObjectNode();
            out.put("lat", lat);
            out.put("lon", lon);
            out.put("grid_id", fullGridId);
            out.put("office", gridId);

            if (existing != null) {
                out.put("source", "db");
                ArrayNode list = fetchHourlyFromDb(fullGridId, limit);
                if (list != null && list.size() > 0) {
                    if (mode != null && mode.equalsIgnoreCase("list")) {
                        out.set("periods", list);
                    } else {
                        out.set("hourly", list.get(0));
                    }
                    try {
                        cacheAndRespond(ctx, cacheKey, out, 300, 600);
                    } catch (Exception e) {
                        ctx.json(out);
                    }
                    return;
                }

                String existingHourlyUrl = existing.forecastHourlyUrl();
                if (existingHourlyUrl != null && !existingHourlyUrl.isBlank()) {
                    ObjectNode outNws = fetchAndStoreHourlyFromNws(fullGridId, existingHourlyUrl, out);
                    try {
                        cacheAndRespond(ctx, cacheKey, outNws, 300, 600);
                    } catch (Exception e) {
                        ctx.json(outNws);
                    }
                    return;
                }
            }

            gridpointRepo.upsertGridpoint(fullGridId, gridId, gridX, gridY, lat, lon, gridDataUrl, forecastHourlyUrl);
            out.put("source", "nws");

            ObjectNode outNws = fetchAndStoreHourlyFromNws(fullGridId, forecastHourlyUrl, out);
            try {
                cacheAndRespond(ctx, cacheKey, outNws, 300, 600);
            } catch (Exception e) {
                ctx.json(outNws);
            }
        });

        // --------------------------------------------------------------------
        // Ingest monitoring endpoints (uses ingest_run / ingest_event tables)
        // --------------------------------------------------------------------
        app.get("/api/ingest/runs", ctx ->

        {
            int limit = parseInt(ctx.queryParam("limit"), 50, 1, 200);

            String sql = """
                        SELECT run_id, job_name, started_at, finished_at, status, notes
                        FROM ingest_run
                        ORDER BY started_at DESC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("run_id", rs.getString("run_id"));
                        row.put("job_name", rs.getString("job_name"));
                        row.put("started_at", String.valueOf(rs.getObject("started_at")));
                        row.put("finished_at", String.valueOf(rs.getObject("finished_at")));
                        row.put("status", rs.getString("status"));
                        row.put("notes", rs.getString("notes"));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        app.get("/api/ingest/events", ctx -> {
            int limit = parseInt(ctx.queryParam("limit"), 200, 1, 1000);
            String runId = ctx.queryParam("runId");

            String sql = """
                        SELECT event_id, run_id, source, endpoint, http_status, response_ms, error, created_at
                        FROM ingest_event
                        WHERE (? IS NULL OR run_id::text = ?)
                        ORDER BY created_at DESC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {

                ps.setString(1, runId);
                ps.setString(2, runId);
                ps.setInt(3, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("event_id", rs.getLong("event_id"));
                        row.put("run_id", String.valueOf(rs.getObject("run_id")));
                        row.put("source", rs.getString("source"));
                        row.put("endpoint", rs.getString("endpoint"));
                        putNullable(row, "http_status", rs.getObject("http_status"));
                        putNullable(row, "response_ms", rs.getObject("response_ms"));
                        row.put("error", rs.getString("error"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        // --------------------------------------------------------------------
        // ML endpoints (read-only)
        // --------------------------------------------------------------------
        app.get("/api/ml/runs", ctx -> {
            int limit = parseInt(ctx.queryParam("limit"), 25, 1, 200);

            String sql = """
                        SELECT run_id, model_name, feature_version, train_start, train_end, dataset_version, split_strategy, created_at
                        FROM ml_model_run
                        ORDER BY created_at DESC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("run_id", rs.getString("run_id"));
                        row.put("model_name", rs.getString("model_name"));
                        row.put("feature_version", rs.getString("feature_version"));
                        row.put("train_start", String.valueOf(rs.getObject("train_start")));
                        row.put("train_end", String.valueOf(rs.getObject("train_end")));
                        row.put("dataset_version", rs.getString("dataset_version"));
                        row.put("split_strategy", rs.getString("split_strategy"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            } catch (Exception e) {
                // If ML tables don’t exist yet in some environments
                log.warn("ML runs query failed (ok if not implemented yet): {}", e.getMessage());
            }

            ctx.json(arr);
        });

        // Latest predictions for a gridpoint:
        // /api/ml/predictions/latest?gridId=LOX:149,46
        app.get("/api/ml/predictions/latest", ctx -> {
            String gridId = firstNonBlank(ctx.queryParam("gridId"), ctx.queryParam("grid_id"));
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId (or grid_id) is required"));
                return;
            }

            int limit = parseInt(ctx.queryParam("limit"), 48, 1, 240);

            // pick newest model_run implicitly by created_at
            String sql = """
                        SELECT p.run_id, p.grid_id, p.valid_time, p.horizon_hours, p.risk_score, p.risk_class, p.created_at
                        FROM ml_prediction p
                        JOIN (
                            SELECT run_id
                            FROM ml_model_run
                            ORDER BY created_at DESC
                            LIMIT 1
                        ) r ON p.run_id = r.run_id
                        WHERE p.grid_id = ?
                        ORDER BY p.valid_time ASC
                        LIMIT ?
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection();
                    PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, gridId);
                ps.setInt(2, limit);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("run_id", rs.getString("run_id"));
                        row.put("grid_id", rs.getString("grid_id"));
                        row.put("valid_time", String.valueOf(rs.getObject("valid_time")));
                        row.put("horizon_hours", rs.getInt("horizon_hours"));
                        putNullable(row, "risk_score", rs.getObject("risk_score"));
                        row.put("risk_class", rs.getString("risk_class"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            } catch (Exception e) {
                log.warn("ML predictions query failed (ok if not implemented yet): {}", e.getMessage());
            }

            ctx.json(arr);
        });

        // Latest ML weather prediction
        // /api/ml/weather/latest?sourceType=point&lat=34.05&lon=-118.4
        app.get("/api/ml/weather/latest", ctx -> {
            String sourceType = ctx.queryParam("sourceType");
            String sourceId = ctx.queryParam("sourceId");
            Double lat = null;
            Double lon = null;
            try {
                if (ctx.queryParam("lat") != null)
                    lat = Double.parseDouble(ctx.queryParam("lat"));
                if (ctx.queryParam("lon") != null)
                    lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }

            if (sourceType == null || sourceType.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "sourceType is required"));
                return;
            }

            // If sourceId is provided, avoid lat/lon filtering so gridpoint/station ids
            // match regardless of pin location.
            if (sourceId != null && !sourceId.isBlank()) {
                lat = null;
                lon = null;
            }

            if ((sourceType.equals("point") || sourceType.equals("station") || sourceType.equals("tracked"))
                    && lat != null && lon != null && !isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "invalid lat/lon"));
                return;
            }

            String sql = """
                            SELECT source_type, source_id, lat, lon, as_of_date, horizon_hours,
                                         tmean_c, prcp_mm, tmin_c, tmax_c, delta_c, model_name, model_detail, confidence, created_at
                            FROM ml_weather_prediction
                            WHERE source_type = ?
                                AND (? IS NULL OR source_id = ?)
                                AND (? IS NULL OR lat BETWEEN ? AND ?)
                                AND (? IS NULL OR lon BETWEEN ? AND ?)
                            ORDER BY created_at DESC
                            LIMIT 1
                    """;

            ObjectNode out = om.createObjectNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, sourceType);
                ps.setString(2, sourceId);
                ps.setString(3, sourceId);

                if (lat != null) {
                    ps.setDouble(4, lat);
                    ps.setDouble(5, lat - 0.001);
                    ps.setDouble(6, lat + 0.001);
                } else {
                    ps.setObject(4, null);
                    ps.setObject(5, null);
                    ps.setObject(6, null);
                }

                if (lon != null) {
                    ps.setDouble(7, lon);
                    ps.setDouble(8, lon - 0.001);
                    ps.setDouble(9, lon + 0.001);
                } else {
                    ps.setObject(7, null);
                    ps.setObject(8, null);
                    ps.setObject(9, null);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        out.put("source_type", rs.getString("source_type"));
                        out.put("source_id", rs.getString("source_id"));
                        putNullable(out, "lat", rs.getObject("lat"));
                        putNullable(out, "lon", rs.getObject("lon"));
                        out.put("as_of_date", String.valueOf(rs.getObject("as_of_date")));
                        putNullable(out, "horizon_hours", rs.getObject("horizon_hours"));
                        putNullable(out, "tmean_c", rs.getObject("tmean_c"));
                        putNullable(out, "prcp_mm", rs.getObject("prcp_mm"));
                        putNullable(out, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(out, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(out, "delta_c", rs.getObject("delta_c"));
                        out.put("model_name", rs.getString("model_name"));
                        out.put("model_detail", rs.getString("model_detail"));
                        putNullable(out, "confidence", rs.getObject("confidence"));
                        out.put("created_at", String.valueOf(rs.getObject("created_at")));
                        ctx.json(out);
                        return;
                    }
                }
            } catch (Exception e) {
                log.warn("ML weather latest query failed: {}", e.getMessage());
            }

            ctx.status(404).json(om.createObjectNode().put("error", "no predictions"));
        });

        // ML weather forecast (10-day) for a source
        // /api/ml/weather/forecast?sourceType=station&sourceId=GHCND:USW00023174
        app.get("/api/ml/weather/forecast", ctx -> {
            String sourceType = ctx.queryParam("sourceType");
            String sourceId = ctx.queryParam("sourceId");
            Double lat = parseDouble(ctx.queryParam("lat"), null);
            Double lon = parseDouble(ctx.queryParam("lon"), null);
            int days = parseInt(ctx.queryParam("days"), 10, 1, 10);

            if (sourceType == null || sourceType.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "sourceType is required"));
                return;
            }

            // If sourceId is provided, avoid lat/lon filtering so gridpoint/station ids
            // match regardless of pin location.
            if (sourceId != null && !sourceId.isBlank()) {
                lat = null;
                lon = null;
            }

            String sql = """
                            SELECT as_of_date, horizon_hours, tmean_c, prcp_mm, tmin_c, tmax_c, delta_c,
                                         model_name, model_detail, confidence, created_at
                            FROM ml_weather_prediction
                            WHERE source_type = ?
                                AND (? IS NULL OR source_id = ?)
                                AND (? IS NULL OR lat BETWEEN ? AND ?)
                                AND (? IS NULL OR lon BETWEEN ? AND ?)
                                AND horizon_hours BETWEEN 0 AND ((? - 1) * 24)
                            ORDER BY horizon_hours ASC
                    """;

            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                int i = 1;
                ps.setString(i++, sourceType);
                ps.setString(i++, sourceId);
                ps.setString(i++, sourceId);

                if (lat != null) {
                    ps.setDouble(i++, lat);
                    ps.setDouble(i++, lat - 0.001);
                    ps.setDouble(i++, lat + 0.001);
                } else {
                    ps.setObject(i++, null);
                    ps.setObject(i++, null);
                    ps.setObject(i++, null);
                }

                if (lon != null) {
                    ps.setDouble(i++, lon);
                    ps.setDouble(i++, lon - 0.001);
                    ps.setDouble(i++, lon + 0.001);
                } else {
                    ps.setObject(i++, null);
                    ps.setObject(i++, null);
                    ps.setObject(i++, null);
                }

                ps.setInt(i, days);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("as_of_date", String.valueOf(rs.getObject("as_of_date")));
                        row.put("horizon_hours", rs.getInt("horizon_hours"));
                        putNullable(row, "tmean_c", rs.getObject("tmean_c"));
                        putNullable(row, "prcp_mm", rs.getObject("prcp_mm"));
                        putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(row, "delta_c", rs.getObject("delta_c"));
                        row.put("model_name", rs.getString("model_name"));
                        row.put("model_detail", rs.getString("model_detail"));
                        putNullable(row, "confidence", rs.getObject("confidence"));
                        row.put("created_at", String.valueOf(rs.getObject("created_at")));
                        arr.add(row);
                    }
                }
            }

            ctx.json(arr);
        });

        // --------------------------------------------------------------------
        // Layer endpoints - simple heatmap layers based on primary station mapping
        // --------------------------------------------------------------------
        app.get("/layers/temperature", ctx -> {
            int hourOffset = parseInt(ctx.queryParam("hourOffset"), 0, 0, 24);
            Instant target = Instant.now().plusSeconds(hourOffset * 3600L);

            ObjectNode fc = featureCollection();
            String sql = """
                                        SELECT g.grid_id, ST_Y(g.geom) AS lat, ST_X(g.geom) AS lon,
                                                     f.temperature_c
                                        FROM geo_gridpoint g
                                        JOIN LATERAL (
                                                SELECT temperature_c
                                                FROM v_latest_hourly_forecast f
                                                WHERE f.grid_id = g.grid_id
                                                    AND f.start_time >= (?::timestamptz - interval '1 hour')
                                                    AND f.start_time < (?::timestamptz + interval '24 hours')
                                                ORDER BY ABS(EXTRACT(EPOCH FROM (f.start_time - ?::timestamptz)))
                                                LIMIT 1
                                        ) f ON true
                                        WHERE g.geom IS NOT NULL
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setObject(1, java.sql.Timestamp.from(target));
                ps.setObject(2, java.sql.Timestamp.from(target));
                ps.setObject(3, java.sql.Timestamp.from(target));
                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");
                        ObjectNode props = om.createObjectNode();
                        props.put("grid_id", rs.getString("grid_id"));
                        putNullable(props, "temperature_c", rs.getObject("temperature_c"));
                        feat.set("properties", props);
                        String geom = String.format("{\"type\":\"Point\",\"coordinates\":[%f,%f]}",
                                rs.getDouble("lon"), rs.getDouble("lat"));
                        feat.set("geometry", om.readTree(geom));
                        feats.add(feat);
                    }
                }
            }

            ctx.json(fc);
        });

        app.get("/layers/precipitation", ctx -> {
            String range = ctx.queryParam("range");
            int days = 30;
            if (range != null && range.endsWith("d")) {
                try {
                    days = Integer.parseInt(range.substring(0, range.length() - 1));
                } catch (Exception ignored) {
                }
            }

            ObjectNode fc = featureCollection();
            String sql = "SELECT s.station_id, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon, SUM(d.prcp_mm) AS prcp_mm "
                    + "FROM noaa_station s "
                    + "JOIN noaa_daily_summary d ON d.station_id = s.station_id "
                    + "WHERE s.geom IS NOT NULL "
                    + "AND d.date >= current_date - (? * INTERVAL '1 day') AND d.date < current_date "
                    + "GROUP BY s.station_id, s.geom";

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, days);
                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");
                        ObjectNode props = om.createObjectNode();
                        props.put("station_id", rs.getString("station_id"));
                        putNullable(props, "prcp_mm", rs.getObject("prcp_mm"));
                        feat.set("properties", props);
                        String geom = String.format("{\"type\":\"Point\",\"coordinates\":[%f,%f]}", rs.getDouble("lon"),
                                rs.getDouble("lat"));
                        feat.set("geometry", om.readTree(geom));
                        feats.add(feat);
                    }
                }
            }

            ctx.json(fc);
        });

        // --------------------------------------------------------------------
        // History endpoints
        // --------------------------------------------------------------------
        app.get("/api/history/daily", ctx -> {
            String stationId = ctx.queryParam("stationId");
            String start = ctx.queryParam("start");
            String end = ctx.queryParam("end");
            if (stationId == null || stationId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "stationId is required"));
                return;
            }

            if (start == null || start.isBlank() || end == null || end.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "start and end dates are required"));
                return;
            }

            String cacheKey = String.format("historyDaily:%s:%s:%s", stationId, start, end);
            if (serveCached(ctx, cacheKey)) {
                return;
            }

            String sql = "SELECT date, tmax_c, tmin_c, prcp_mm FROM noaa_daily_summary WHERE station_id = ? AND date >= ?::date AND date <= ?::date ORDER BY date";
            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, stationId);
                ps.setString(2, start);
                ps.setString(3, end);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("date", String.valueOf(rs.getObject("date")));
                        putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(row, "prcp_mm", rs.getObject("prcp_mm"));
                        arr.add(row);
                    }
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, arr, 21600, 43200);
            } catch (Exception e) {
                ctx.json(arr);
            }
        });

        app.get("/api/history/gridpoint", ctx -> {
            String gridId = ctx.queryParam("gridId");
            int days = parseInt(ctx.queryParam("days"), 365, 1, 3650);
            if (gridId == null || gridId.isBlank()) {
                ctx.status(400).json(om.createObjectNode().put("error", "gridId is required"));
                return;
            }

            String cacheKey = String.format("historyGridpoint:%s:%d", gridId, days);
            if (serveCached(ctx, cacheKey)) {
                return;
            }

            String stationSql = "SELECT station_id FROM gridpoint_station_map WHERE grid_id = ? AND is_primary = true LIMIT 1";
            String stationId = null;
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(stationSql)) {
                ps.setString(1, gridId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next())
                        stationId = rs.getString(1);
                }
            }

            if (stationId == null) {
                ctx.status(404).json(om.createObjectNode().put("error", "no primary station for gridId"));
                return;
            }

            java.time.LocalDate endDate = java.time.LocalDate.now().minusDays(1);
            java.time.LocalDate startDate = endDate.minusDays(days - 1);

            String sql = "SELECT date, tmax_c, tmin_c, prcp_mm FROM noaa_daily_summary WHERE station_id = ? AND date >= ? AND date <= ? ORDER BY date";
            ArrayNode arr = om.createArrayNode();
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, stationId);
                ps.setObject(2, startDate);
                ps.setObject(3, endDate);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        row.put("date", String.valueOf(rs.getObject("date")));
                        putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(row, "prcp_mm", rs.getObject("prcp_mm"));
                        arr.add(row);
                    }
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, arr, 3600, 7200);
            } catch (Exception e) {
                ctx.json(arr);
            }
        });

        // --------------------------------------------------------------------
        // Point summary: nearest stations + interpolated historic + nearest gridpoint
        // hourly
        // --------------------------------------------------------------------
        app.get("/api/point/summary", ctx -> {
            Double lat = null;
            Double lon = null;
            try {
                lat = Double.parseDouble(ctx.queryParam("lat"));
                lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }

            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }

            int days = parseInt(ctx.queryParam("days"), 30, 1, 3650);
            int limit = parseInt(ctx.queryParam("limit"), 10, 1, 50);

            String cacheKey = String.format("pointSummary:%.4f,%.4f:%d:%d", roundTo(lat, 4), roundTo(lon, 4), days,
                    limit);
            if (serveCached(ctx, cacheKey)) {
                return;
            }

            ObjectNode out = om.createObjectNode();
            ObjectNode q = om.createObjectNode();
            q.put("lat", lat);
            q.put("lon", lon);
            q.put("days", days);
            out.set("query", q);

            ArrayNode stations = om.createArrayNode();
            Double interpTmean = null;
            Double interpPrcp = null;
            Double weightSumT = 0.0;
            Double weightSumP = 0.0;
            Double tmeanWeighted = 0.0;
            Double prcpWeighted = 0.0;

            String stationSql = """
                    WITH nearest AS (
                        SELECT station_id, name,
                               ST_Y(geom) AS lat,
                               ST_X(geom) AS lon,
                               ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS dist_m
                        FROM noaa_station
                        WHERE geom IS NOT NULL
                        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)
                        LIMIT ?
                    ), daily AS (
                        SELECT n.*,
                               d.date AS latest_date,
                               d.tmax_c, d.tmin_c, d.prcp_mm AS prcp_latest_mm,
                               p.prcp_window_mm,
                               cov.rows_total, cov.rows_tmax, cov.rows_tmin, cov.rows_prcp,
                               cov.first_date, cov.last_date
                        FROM nearest n
                        LEFT JOIN LATERAL (
                            SELECT date, tmax_c, tmin_c, prcp_mm
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                            ORDER BY date DESC
                            LIMIT 1
                        ) d ON true
                        LEFT JOIN LATERAL (
                            SELECT SUM(prcp_mm) AS prcp_window_mm
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                              AND date >= current_date - (? * INTERVAL '1 day')
                              AND date < current_date
                        ) p ON true
                        LEFT JOIN LATERAL (
                            SELECT COUNT(*) AS rows_total,
                                   COUNT(tmax_c) AS rows_tmax,
                                   COUNT(tmin_c) AS rows_tmin,
                                   COUNT(prcp_mm) AS rows_prcp,
                                   MIN(date) AS first_date,
                                   MAX(date) AS last_date
                            FROM noaa_daily_summary
                            WHERE station_id = n.station_id
                        ) cov ON true
                    )
                    SELECT * FROM daily
                    """;

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(stationSql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                ps.setDouble(3, lon);
                ps.setDouble(4, lat);
                ps.setInt(5, limit);
                ps.setInt(6, days);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ObjectNode row = om.createObjectNode();
                        String stationId = rs.getString("station_id");
                        row.put("station_id", stationId);
                        row.put("name", rs.getString("name"));
                        putNullable(row, "lat", rs.getObject("lat"));
                        putNullable(row, "lon", rs.getObject("lon"));
                        double distM = rs.getDouble("dist_m");
                        row.put("dist_km", distM / 1000.0);
                        row.put("latest_date", String.valueOf(rs.getObject("latest_date")));
                        putNullable(row, "tmax_c", rs.getObject("tmax_c"));
                        putNullable(row, "tmin_c", rs.getObject("tmin_c"));
                        putNullable(row, "prcp_latest_mm", rs.getObject("prcp_latest_mm"));
                        putNullable(row, "prcp_window_mm", rs.getObject("prcp_window_mm"));
                        putNullable(row, "rows_total", rs.getObject("rows_total"));
                        putNullable(row, "rows_tmax", rs.getObject("rows_tmax"));
                        putNullable(row, "rows_tmin", rs.getObject("rows_tmin"));
                        putNullable(row, "rows_prcp", rs.getObject("rows_prcp"));
                        putNullable(row, "first_date", rs.getObject("first_date"));
                        putNullable(row, "last_date", rs.getObject("last_date"));
                        if (rs.getObject("tmax_c") != null && rs.getObject("tmin_c") != null) {
                            double tmean = (rs.getDouble("tmax_c") + rs.getDouble("tmin_c")) / 2.0;
                            row.put("tmean_c", tmean);
                        }
                        stations.add(row);

                        Double tmax = rs.getObject("tmax_c") == null ? null : rs.getDouble("tmax_c");
                        Double tmin = rs.getObject("tmin_c") == null ? null : rs.getDouble("tmin_c");
                        Double prcpWindow = rs.getObject("prcp_window_mm") == null ? null
                                : rs.getDouble("prcp_window_mm");

                        double w = 1.0 / Math.max(distM, 1.0);
                        if (tmax != null && tmin != null) {
                            double tmean = (tmax + tmin) / 2.0;
                            tmeanWeighted += w * tmean;
                            weightSumT += w;
                        }
                        if (prcpWindow != null) {
                            prcpWeighted += w * prcpWindow;
                            weightSumP += w;
                        }
                    }
                }
            }

            if (weightSumT > 0) {
                interpTmean = tmeanWeighted / weightSumT;
            }
            if (weightSumP > 0) {
                interpPrcp = prcpWeighted / weightSumP;
            }

            out.set("nearest_stations", stations);
            ObjectNode interpolated = om.createObjectNode();
            interpolated.put("days", days);
            putNullable(interpolated, "tmean_c", interpTmean);
            putNullable(interpolated, "prcp_window_mm", interpPrcp);
            out.set("interpolated", interpolated);

            // Nearest gridpoint + latest hourly data
            String gridSql = "SELECT grid_id, office, grid_x, grid_y, ST_Y(geom) AS lat, ST_X(geom) AS lon " +
                    "FROM geo_gridpoint WHERE geom IS NOT NULL " +
                    "ORDER BY geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326) LIMIT 1";

            ObjectNode nearestGrid = null;
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(gridSql)) {
                ps.setDouble(1, lon);
                ps.setDouble(2, lat);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        nearestGrid = om.createObjectNode();
                        nearestGrid.put("grid_id", rs.getString("grid_id"));
                        nearestGrid.put("office", rs.getString("office"));
                        nearestGrid.put("grid_x", rs.getInt("grid_x"));
                        nearestGrid.put("grid_y", rs.getInt("grid_y"));
                        putNullable(nearestGrid, "lat", rs.getObject("lat"));
                        putNullable(nearestGrid, "lon", rs.getObject("lon"));
                    }
                }
            }

            if (nearestGrid != null) {
                out.set("nearest_gridpoint", nearestGrid);

                String hourlySql = """
                        SELECT start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg,
                               precip_prob, relative_humidity, short_forecast
                        FROM v_latest_hourly_forecast
                        WHERE grid_id = ?
                        ORDER BY start_time ASC
                        LIMIT 1
                        """;

                ObjectNode hourly = null;
                try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(hourlySql)) {
                    ps.setString(1, nearestGrid.get("grid_id").asText());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            hourly = om.createObjectNode();
                            hourly.put("start_time", String.valueOf(rs.getObject("start_time")));
                            hourly.put("end_time", String.valueOf(rs.getObject("end_time")));
                            putNullable(hourly, "temperature_c", rs.getObject("temperature_c"));
                            putNullable(hourly, "wind_speed_mps", rs.getObject("wind_speed_mps"));
                            putNullable(hourly, "wind_gust_mps", rs.getObject("wind_gust_mps"));
                            putNullable(hourly, "wind_dir_deg", rs.getObject("wind_dir_deg"));
                            putNullable(hourly, "precip_prob", rs.getObject("precip_prob"));
                            putNullable(hourly, "relative_humidity", rs.getObject("relative_humidity"));
                            hourly.put("short_forecast", rs.getString("short_forecast"));
                        }
                    }
                }

                if (hourly != null) {
                    out.set("hourly", hourly);
                }
            }

            try {
                cacheAndRespond(ctx, cacheKey, out, 300, 600);
            } catch (Exception e) {
                ctx.json(out);
            }
        });

        app.start(cfg.apiPort());
    }

    public void stop() {
        if (app != null)
            app.stop();
        log.info("API server stopped");
    }

    // --------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------
    private static class CachedResponse {
        final String body;
        final long expiresAt;
        final String etag;
        final int maxAgeSeconds;
        final int staleSeconds;

        CachedResponse(String body, long expiresAt, String etag, int maxAgeSeconds, int staleSeconds) {
            this.body = body;
            this.expiresAt = expiresAt;
            this.etag = etag;
            this.maxAgeSeconds = maxAgeSeconds;
            this.staleSeconds = staleSeconds;
        }
    }

    private boolean serveCached(Context ctx, String key) {
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

    private void cacheAndRespond(Context ctx, String key, JsonNode node, int maxAgeSeconds, int staleSeconds)
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

    private void applyCacheHeaders(Context ctx, int maxAgeSeconds, int staleSeconds) {
        if (maxAgeSeconds <= 0) {
            return;
        }
        StringBuilder sb = new StringBuilder("public, max-age=").append(maxAgeSeconds);
        if (staleSeconds > 0) {
            sb.append(", stale-while-revalidate=").append(staleSeconds);
        }
        ctx.header("Cache-Control", sb.toString());
    }

    private ObjectNode featureCollection() {
        ObjectNode fc = om.createObjectNode();
        fc.put("type", "FeatureCollection");
        fc.set("features", om.createArrayNode());
        return fc;
    }

    private static double roundTo(double value, int places) {
        if (places < 0)
            return value;
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    private static String bboxKey(double[] bbox) {
        if (bbox == null || bbox.length != 4)
            return "invalid";
        return String.format("%.3f,%.3f,%.3f,%.3f",
                roundTo(bbox[0], 3),
                roundTo(bbox[1], 3),
                roundTo(bbox[2], 3),
                roundTo(bbox[3], 3));
    }

    private double[] parseBbox(String bbox) {
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

    private static boolean isLatLonValid(Double lat, Double lon) {
        if (lat == null || lon == null)
            return false;
        return lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0;
    }

    private void putNullable(ObjectNode obj, String key, Object value) {
        if (value == null)
            obj.putNull(key);
        else if (value instanceof Number n)
            obj.put(key, n.doubleValue());
        else
            obj.put(key, value.toString());
    }

    private static Integer parseInt(String s, Integer def, int min, int max) {
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

    private static Double parseDouble(String s, Double def) {
        if (s == null || s.isBlank())
            return def;
        try {
            return Double.parseDouble(s.trim());
        } catch (Exception e) {
            return def;
        }
    }

    private static Double toCelsius(JsonNode period) {
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

    private static Double parseWindSpeedMps(String windText) {
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

    private static Double parsePrecipProb01(JsonNode probNode) {
        if (probNode == null || probNode.isMissingNode())
            return null;
        JsonNode v = probNode.get("value");
        if (v == null || v.isNull())
            return null;
        double percent = v.asDouble();
        return percent / 100.0;
    }

    private static Double parseValueNode(JsonNode node) {
        if (node == null || node.isMissingNode())
            return null;
        JsonNode v = node.get("value");
        if (v == null || v.isNull())
            return null;
        return v.asDouble();
    }

    private static Double cardinalToDegrees(String dir) {
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

    private ArrayNode fetchHourlyFromDb(String gridId, int limit) throws Exception {
        String sql = """
                SELECT start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg,
                       precip_prob, relative_humidity, short_forecast
                FROM v_latest_hourly_forecast
                WHERE grid_id = ?
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

    private ObjectNode fetchAndStoreHourlyFromNws(String gridId, String forecastHourlyUrl, ObjectNode out)
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

    private Instant parseInstant(String s) {
        if (s == null || s.isBlank())
            return null;
        try {
            return Instant.parse(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank())
            return a;
        if (b != null && !b.isBlank())
            return b;
        return null;
    }
}
