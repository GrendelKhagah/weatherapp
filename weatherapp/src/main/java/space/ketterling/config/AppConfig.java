package space.ketterling.config;

import java.io.InputStream;
import java.time.*;
import java.util.*;

/**
 * Application configuration loaded from environment variables or properties.
 *
 * <p>
 * This record groups all runtime settings for the API, database, NOAA/NWS,
 * ingest schedules, and ML service.
 * </p>
 */
public record AppConfig(
        // API / DB / NWS
        int apiPort,
        String dbJdbcUrl,
        String dbUsername,
        String dbPassword,
        int dbApiPoolMax,
        int dbIngestPoolMax,
        String nwsUserAgent,
        List<double[]> trackedPoints, // each = [lat, lon]
        Duration gridpointRefresh,
        Duration hourlyForecast,
        Duration alerts,

        // NOAA
        boolean noaaApiEnabled,
        String noaaToken,
        double noaaStationRadiusKm,
        int noaaStationLimit,
        int noaaMapKeep,
        LocalDate noaaBackfillStart,
        int noaaHistoryChunkDays,
        Duration noaaStationsRefresh,
        Duration noaaDailyHistory,

        // Time
        ZoneId clockZoneId,

        // Local station historic data directory address
        String stationHistoricDir,

        // ML
        boolean mlEnabled,
        Duration mlSchedule,
        String mlDefaultModelName,
        String mlFeatureVersion,
        String mlArtifactPath,

        // Local ingest state file for station-historic CSV ingest
        String stationHistoricStateFile) {

    /**
     * Loads configuration using environment variables, system properties, and
     * application.properties (in that order).
     */
    public static AppConfig load() {
        Properties p = new Properties();
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null)
                p.load(in);
        } catch (Exception ignored) {
        }

        // Required-ish
        String dbUrl = requireNonBlank(envOr(p, "DB_JDBC_URL", "db.jdbcUrl", ""));
        String dbUser = requireNonBlank(envOr(p, "DB_USERNAME", "db.username", ""));
        String dbPass = envOr(p, "DB_PASSWORD", "db.password", ""); // ok empty if local trust auth
        String ua = requireNonBlank(envOr(p, "NWS_USER_AGENT", "nws.userAgent", ""));

        int dbApiPoolMax = Integer.parseInt(envOr(p, "DB_API_POOL_MAX", "db.api.poolMax", "8"));
        int dbIngestPoolMax = Integer.parseInt(envOr(p, "DB_INGEST_POOL_MAX", "db.ingest.poolMax", "12"));

        // API / tracking
        int port = Integer.parseInt(envOr(p, "API_PORT", "api.port", "8080"));
        List<double[]> points = parsePoints(envOr(p, "TRACKED_POINTS", "tracked.points", ""));

        // Schedules
        Duration gp = Duration.parse(envOr(p, "SCHED_GRIDPOINT", "schedule.gridpointRefresh", "PT24H"));
        Duration hf = Duration.parse(envOr(p, "SCHED_HOURLY", "schedule.hourlyForecast", "PT30M"));
        Duration al = Duration.parse(envOr(p, "SCHED_ALERTS", "schedule.alerts", "PT5M"));

        // NOAA
        boolean noaaApiEnabled = Boolean.parseBoolean(envOr(p, "NOAA_API_ENABLED", "noaa.api.enabled", "true"));
        String noaaToken = envOr(p, "NOAA_TOKEN", "noaa.token", "");
        double stationRadiusKm = Double.parseDouble(envOr(p, "NOAA_STATION_RADIUS_KM", "noaa.stationRadiusKm", "50"));
        int stationLimit = Integer.parseInt(envOr(p, "NOAA_STATION_LIMIT", "noaa.stationLimit", "25"));
        int mapKeep = Integer.parseInt(envOr(p, "NOAA_MAP_KEEP", "noaa.mapKeep", "5"));

        LocalDate backfillStart = LocalDate.parse(envOr(p, "NOAA_BACKFILL_START", "noaa.backfillStart", "2016-01-01"));
        int chunkDays = Integer.parseInt(envOr(p, "NOAA_HISTORY_CHUNK_DAYS", "noaa.historyChunkDays", "365"));

        Duration noaaStationsRefresh = Duration
                .parse(envOr(p, "SCHED_NOAA_STATIONS", "schedule.noaaStationsRefresh", "P7D"));
        Duration noaaDailyHistory = Duration.parse(envOr(p, "SCHED_NOAA_DAILY", "schedule.noaaDailyHistory", "P1D"));

        // Local historic CSV ingest
        String stationHistoricDir = envOr(
                p,
                "STATION_HISTORIC_DIR",
                "stationHistoric.dir",
                "/opt/weather-app/data/stationHistoricData");

        String stationHistoricStateFile = envOr(
                p,
                "STATION_HISTORIC_STATE_FILE",
                "stationHistoric.stateFile",
                "/opt/weather-app/data/stationHistoricIngest.state");

        // Time zone
        ZoneId zoneId = ZoneId.of(envOr(p, "CLOCK_ZONE", "clock.zone", "America/Los_Angeles"));

        // ML
        boolean mlEnabled = Boolean.parseBoolean(envOr(p, "ML_ENABLED", "ml.enabled", "false"));
        Duration mlSchedule = Duration.parse(envOr(p, "SCHED_ML", "schedule.ml", "P1D"));
        String mlModelName = envOr(p, "ML_MODEL_NAME", "ml.modelName", "baseline-risk");
        String mlFeatureVersion = envOr(p, "ML_FEATURE_VERSION", "ml.featureVersion", "v1");
        String mlArtifactPath = envOr(p, "ML_ARTIFACT_PATH", "ml.artifactPath", "/opt/weather-app/ml");

        // IMPORTANT: constructor args must match record field order exactly
        return new AppConfig(
                port,
                dbUrl,
                dbUser,
                dbPass,
                dbApiPoolMax,
                dbIngestPoolMax,
                ua,
                points,
                gp,
                hf,
                al,

                noaaApiEnabled,
                noaaToken,
                stationRadiusKm,
                stationLimit,
                mapKeep,
                backfillStart,
                chunkDays,
                noaaStationsRefresh,
                noaaDailyHistory,

                zoneId,

                stationHistoricDir,

                mlEnabled,
                mlSchedule,
                mlModelName,
                mlFeatureVersion,
                mlArtifactPath,

                stationHistoricStateFile);
    }

    // ----------------------------
    // helpers
    // ----------------------------
    /**
     * Reads a value from env, then JVM property, then properties file fallback.
     */
    private static String envOr(Properties p, String envKey, String propKey, String def) {
        String v = System.getenv(envKey);
        if (v != null && !v.isBlank())
            return v;
        // optional: allow -Dprop=... override too
        String sys = System.getProperty(propKey);
        if (sys != null && !sys.isBlank())
            return sys;
        return p.getProperty(propKey, def);
    }

    /**
     * Ensures a required config value is present and not blank.
     */
    private static String requireNonBlank(String v) {
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(
                    "Missing required config value (env var, -Dprop, or application.properties).");
        }
        return v;
    }

    /**
     * Parses tracked points in the format "lat,lon|lat,lon".
     */
    private static List<double[]> parsePoints(String s) {
        if (s == null || s.isBlank())
            return List.of();
        List<double[]> out = new ArrayList<>();
        for (String part : s.split("\\|")) {
            String[] bits = part.trim().split(",");
            if (bits.length != 2)
                continue;
            double lat = Double.parseDouble(bits[0].trim());
            double lon = Double.parseDouble(bits[1].trim());
            out.add(new double[] { lat, lon });
        }
        return out;
    }
}
