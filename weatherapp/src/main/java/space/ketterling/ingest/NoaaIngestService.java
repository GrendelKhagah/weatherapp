package space.ketterling.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.ketterling.config.AppConfig;
import space.ketterling.db.*;
import space.ketterling.noaa.NoaaClient;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads NOAA station metadata and daily history into the database.
 *
 * <p>
 * This service handles two main jobs:
 * <ul>
 * <li>Find nearby NOAA stations for each gridpoint and store the mapping.</li>
 * <li>Pull daily temperature/precipitation history for mapped stations.</li>
 * </ul>
 */
public class NoaaIngestService {
    private static final Logger log = LoggerFactory.getLogger(NoaaIngestService.class);
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_LOCAL_DATE;

    private final AppConfig cfg;
    private final ObjectMapper om;
    private final NoaaClient noaa;

    private final IngestLogRepo logRepo;
    private final NoaaStationRepo stationRepo;
    private final NoaaDailyRepo dailyRepo;
    private final GridpointStationMapRepo mapRepo;

    // we reuse GridpointRepo to list gridpoints (we’ll add a small method below)
    private final GridpointRepo gridRepo;
    private final CachedGridAggRepo cacheRepo;

    /**
     * Creates a NOAA ingest service with required clients and repositories.
     */
    public NoaaIngestService(
            AppConfig cfg,
            ObjectMapper om,
            NoaaClient noaa,
            IngestLogRepo logRepo,
            NoaaStationRepo stationRepo,
            NoaaDailyRepo dailyRepo,
            GridpointStationMapRepo mapRepo,
            GridpointRepo gridRepo) {
        this.cfg = cfg;
        this.om = om;
        this.noaa = noaa;
        this.logRepo = logRepo;
        this.stationRepo = stationRepo;
        this.dailyRepo = dailyRepo;
        this.mapRepo = mapRepo;
        this.gridRepo = gridRepo;
        this.cacheRepo = new CachedGridAggRepo(Database.createIngestDataSource(cfg));
    }

    /**
     * Updates cached grid aggregates for a given date window.
     *
     * <p>
     * This is called by the scheduler after ingesting new daily data.
     * </p>
     */
    public void populateCache(java.time.LocalDate asOf, int windowDays) throws Exception {
        cacheRepo.upsertAggregates(asOf, windowDays);
    }

    // -------------------------------------------------------
    // JOB A: Discover stations near each gridpoint + map them
    // -------------------------------------------------------
    /**
     * Finds NOAA stations near each gridpoint and saves the closest ones.
     *
     * <p>
     * If a local GHCN station file exists, it uses that file to avoid API calls.
     * </p>
     */
    public void refreshStationsAndMapping() throws Exception {
        log.info("Starting job: refreshStationsAndMapping");
        UUID runId = logRepo.startRun("noaa_station_discovery");
        int ok = 0, fail = 0;

        try {
            // Prefer local GHCN stations file if present. This lets us scope to California
            // and avoid
            // remote station searches for large backfills.
            var stationsStream = NoaaIngestService.class.getClassLoader()
                    .getResourceAsStream("noaaData/ghcnd-stations.txt");
            List<StationRecord> localStations = null;
            if (stationsStream != null) {
                localStations = new ArrayList<>();
                try (var br = new java.io.BufferedReader(new java.io.InputStreamReader(stationsStream))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isBlank())
                            continue;
                        // ghcnd-stations.txt format: id, lat, lon, elevation, rest (name/state)
                        String[] parts = line.trim().split("\\s+", 5);
                        if (parts.length < 3)
                            continue;
                        String id = parts[0];
                        double sLat = Double.parseDouble(parts[1]);
                        double sLon = Double.parseDouble(parts[2]);
                        Double elev = null;
                        if (parts.length >= 4) {
                            try {
                                elev = Double.parseDouble(parts[3]);
                            } catch (Exception ignored) {
                            }
                        }
                        String name = parts.length >= 5 ? parts[4].trim() : null;

                        // Filter to California bounding box
                        if (sLat >= 32.5 && sLat <= 42.0 && sLon >= -124.5 && sLon <= -114.0) {
                            localStations.add(new StationRecord(id, sLat, sLon, elev, name));
                        }
                    }
                }
                log.info("Using local GHCN stations file for mapping (CA filter applied), localStations={}",
                        localStations.size());
            }

            var gridpoints = gridRepo.listGridpointsWithLatLon();
            // If we have local stations, map each gridpoint to nearest local stations;
            // otherwise fall back to NOAA API per-gridpoint
            for (var gp : gridpoints) {
                String gridId = gp.gridId();
                double lat = gp.lat();
                double lon = gp.lon();

                try {
                    if (localStations != null) {
                        // Compute distances to local stations and pick nearest
                        List<StationPick> picks = new ArrayList<>();
                        for (var s : localStations) {
                            double distM = haversineMeters(lat, lon, s.lat, s.lon);
                            if (distM <= cfg.noaaStationRadiusKm() * 1000.0) {
                                picks.add(new StationPick(s.id, distM, s.lat, s.lon, s.elev, s.name));
                            }
                        }

                        if (picks.isEmpty()) {
                            log.warn("No local stations within radius for grid {}", gridId);
                            fail++;
                            continue;
                        }

                        picks.sort(Comparator.comparingDouble(p -> p.distanceM));
                        StationPick best = picks.get(0);

                        mapRepo.clearPrimary(gridId);
                        int keep = Math.min(cfg.noaaMapKeep(), picks.size());
                        for (int i = 0; i < keep; i++) {
                            StationPick p = picks.get(i);
                            boolean isPrimary = i == 0;
                            // upsert station metadata
                            stationRepo.upsertStation(p.stationId, p.name, p.lat, p.lon, p.elevation,
                                    om.writeValueAsString(Map.of("source", "local-ghcnd")));
                            mapRepo.upsertMap(gridId, p.stationId, p.distanceM, isPrimary);
                        }

                        logRepo.logEvent(runId, "NOAA_LOCAL", "ghcnd-stations", 200, 0, null, Map.of());
                        log.info("Mapped gridId={} using LOCAL GHCN CSV -> primaryStation={} dist_m={}", gridId,
                                best.stationId,
                                best.distanceM);
                        ok++;
                    } else {
                        // remote NOAA station search
                        log.info("Querying NOAA stations API for gridId={} lat={} lon={} (radiusKm={})", gridId, lat,
                                lon, cfg.noaaStationRadiusKm());
                        // fallback to remote NOAA station search
                        String endpoint = "NOAA stationsNear lat=" + lat + " lon=" + lon;
                        long t0 = System.currentTimeMillis();
                        JsonNode root = noaa.stationsNear(lat, lon, cfg.noaaStationRadiusKm(), cfg.noaaStationLimit());
                        int ms = (int) (System.currentTimeMillis() - t0);

                        JsonNode results = root.path("results");
                        if (!results.isArray() || results.size() == 0) {
                            throw new IllegalStateException("No stations returned for grid " + gridId);
                        }

                        StationPick best = null;
                        List<StationPick> picks = new ArrayList<>();
                        for (JsonNode s : results) {
                            String id = s.path("id").asText(null);
                            if (id == null)
                                continue;

                            Double sLat = getDoubleOrNull(s.get("latitude"));
                            Double sLon = getDoubleOrNull(s.get("longitude"));
                            Double elev = getDoubleOrNull(s.get("elevation"));
                            String name = s.path("name").asText(null);

                            stationRepo.upsertStation(id, name, sLat, sLon, elev, om.writeValueAsString(s));

                            Double distM = (sLat == null || sLon == null) ? null
                                    : haversineMeters(lat, lon, sLat, sLon);
                            StationPick pick = new StationPick(id, distM);
                            picks.add(pick);

                            if (best == null)
                                best = pick;
                            else if (distM != null && best.distanceM != null && distM < best.distanceM)
                                best = pick;
                            else if (best.distanceM == null && distM != null)
                                best = pick;
                        }

                        if (best == null)
                            throw new IllegalStateException("No valid stations in results");

                        mapRepo.clearPrimary(gridId);
                        picks.sort(Comparator
                                .comparingDouble(p -> p.distanceM == null ? Double.POSITIVE_INFINITY : p.distanceM));
                        int keep = Math.min(cfg.noaaMapKeep(), picks.size());
                        for (int i = 0; i < keep; i++) {
                            StationPick p = picks.get(i);
                            boolean isPrimary = p.stationId.equals(best.stationId);
                            mapRepo.upsertMap(gridId, p.stationId, p.distanceM, isPrimary);
                        }

                        logRepo.logEvent(runId, "NOAA", endpoint, 200, ms, null, Map.of());
                        ok++;
                        log.info("Mapped gridId={} -> primaryStation={} dist_m={}", gridId, best.stationId,
                                best.distanceM);
                    }
                } catch (Exception e) {
                    logRepo.logEvent(runId, "NOAA", "station_mapping", null, null, e.getMessage(), Map.of());
                    fail++;
                    log.warn("Station mapping failed for gridId={} err={}", gridId, e.getMessage());
                }
            }

            logRepo.finishRun(runId, fail == 0, "ok=" + ok + " fail=" + fail);
            log.info("Finished refreshStationsAndMapping: ok={} fail={}", ok, fail);
        } catch (Exception outer) {
            logRepo.finishRun(runId, false, "fatal: " + outer.getMessage());
            throw outer;
        }
    }

    // -------------------------------------------------------
    // JOB B: Pull GHCND daily history for primary stations
    // -------------------------------------------------------
    /**
     * Pulls daily GHCND history for each primary station and writes it to the DB.
     *
     * <p>
     * Skips stations that have local CSV history to reduce NOAA API usage.
     * </p>
     */
    public void ingestDailyHistory() throws Exception {
        log.info("Starting job: ingestDailyHistory");
        UUID runId = logRepo.startRun("noaa_daily_history");
        int ok = 0, fail = 0;

        try {
            var primaryStations = gridRepo.listPrimaryStations();
            LocalDate today = LocalDate.now(cfg.clockZoneId());
            LocalDate end = today.minusDays(1); // ingest through yesterday

            for (String stationId : primaryStations) {
                try {
                    processStation(runId, stationId, end);
                    ok++;
                } catch (Exception e) {
                    String endpoint = "NOAA daily station=" + stationId;
                    logRepo.logEvent(runId, "NOAA", endpoint, null, null, e.getMessage(), Map.of());
                    fail++;
                    log.warn("Daily ingest failed station={} err={}", stationId, e.getMessage());

                    // Fallback: try to find alternate primary(s) for gridpoints that map to this
                    // station
                    try {
                        var gridIds = gridRepo.getGridpointIdsForStation(stationId);
                        for (String gridId : gridIds) {
                            String alt = mapRepo.getAlternatePrimary(gridId, stationId);
                            if (alt != null && !alt.equals(stationId)) {
                                log.info("Attempting fallback station {} for grid {} (failed primary={})", alt, gridId,
                                        stationId);
                                try {
                                    processStation(runId, alt, end);
                                } catch (Exception ex2) {
                                    log.warn("Fallback ingest also failed station={} err={}", alt, ex2.getMessage());
                                }
                            }
                        }
                    } catch (Exception ex) {
                        log.debug("Fallback lookup failed for station {}: {}", stationId, ex.getMessage());
                    }
                }
            }

            // After ingesting daily history, update cached aggregates for yesterday
            try {
                java.time.LocalDate asOf = java.time.LocalDate.now(cfg.clockZoneId()).minusDays(1);
                cacheRepo.upsertAggregates(asOf, 30);
            } catch (Exception e) {
                log.warn("Failed to populate cached_grid_agg: {}", e.getMessage());
            }

            logRepo.finishRun(runId, fail == 0, "ok=" + ok + " fail=" + fail);
            log.info("Finished ingestDailyHistory: ok={} fail={}", ok, fail);
        } catch (Exception outer) {
            logRepo.finishRun(runId, false, "fatal: " + outer.getMessage());
            throw outer;
        }
    }

    /**
     * Ingests daily records for a single station in date-range chunks.
     */
    private void processStation(UUID runId, String stationId, LocalDate end) throws Exception {
        LocalDate maxExisting = dailyRepo.maxDateForStation(stationId);
        LocalDate start = (maxExisting == null) ? cfg.noaaBackfillStart() : maxExisting.plusDays(1);

        if (start.isAfter(end)) {
            log.info("Daily history up-to-date for {} (max={})", stationId, maxExisting);
            return;
        }

        // If a local CSV exists for this station, skip NOAA API calls (local historic
        // data preferred).
        if (hasLocalCsv(stationId)) {
            log.info("Skipping NOAA daily ingest for {}: local CSV available", stationId);
            return;
        }

        // chunk the date range so we don't request a massive span at once
        LocalDate chunkStart = start;
        while (!chunkStart.isAfter(end)) {
            LocalDate chunkEnd = chunkStart.plusDays(cfg.noaaHistoryChunkDays() - 1L);
            if (chunkEnd.isAfter(end))
                chunkEnd = end;

            ingestStationChunk(runId, stationId, chunkStart, chunkEnd);

            chunkStart = chunkEnd.plusDays(1);
        }
    }

    /**
     * Checks common resource and filesystem locations for a station CSV file.
     */
    private boolean hasLocalCsv(String stationId) {
        String raw = stationId.startsWith("GHCND:") ? stationId.substring("GHCND:".length()) : stationId;
        String resourcePath = "stationHistoricData/" + raw + ".csv";
        var cl = NoaaIngestService.class.getClassLoader();
        if (cl.getResource(resourcePath) != null)
            return true;
        // also check common filesystem locations
        Path p1 = Path.of("src/main/resources", "stationHistoricData", raw + ".csv");
        Path p2 = Path.of("target/classes", "stationHistoricData", raw + ".csv");
        Path p3 = Path.of("stationHistoricData", raw + ".csv");
        if (Files.exists(p1) || Files.exists(p2) || Files.exists(p3))
            return true;

        // Check configured stationHistoricDir, including date subdirectories
        try {
            Path base = Path.of(cfg.stationHistoricDir());
            if (Files.exists(base)) {
                if (Files.exists(base.resolve(raw + ".csv")))
                    return true;
                try (var stream = Files.list(base)) {
                    for (Path sub : stream.toList()) {
                        if (!Files.isDirectory(sub))
                            continue;
                        if (Files.exists(sub.resolve(raw + ".csv")))
                            return true;
                    }
                }
            }
        } catch (Exception ignored) {
        }

        return false;
    }

    /**
     * Downloads a small date range from NOAA and writes aggregated daily rows.
     */
    private void ingestStationChunk(UUID runId, String stationId, LocalDate start, LocalDate end) throws Exception {
        String startStr = start.format(ISO);
        String endStr = end.format(ISO);

        int limit = Math.min(250, 1000); // reduce page size to lower per-request load
        int offset = 1;

        int pages = 0;
        int rows = 0;

        while (true) {
            long t0 = System.currentTimeMillis();
            log.debug("Fetching NOAA daily data via NOAA API for station={} range={}..{}", stationId, startStr,
                    endStr);
            JsonNode root = noaa.dailyGhcnd(stationId, startStr, endStr, limit, offset);
            int ms = (int) (System.currentTimeMillis() - t0);

            JsonNode results = root.path("results");
            if (!results.isArray() || results.size() == 0) {
                // no data in this span
                logRepo.logEvent(runId, "NOAA", "dailyGhcnd " + stationId + " " + startStr + ".." + endStr, 200, ms,
                        null, Map.of());
                break;
            }

            // Each row is one datatype for one date. We group by date, then write one daily
            // row.
            Map<LocalDate, DayAgg> agg = new HashMap<>();
            for (JsonNode r : results) {
                String dateTime = r.path("date").asText(null); // e.g. 2026-01-01T00:00:00
                String datatype = r.path("datatype").asText(null); // TMAX/TMIN/PRCP
                Double value = getDoubleOrNull(r.get("value"));

                if (dateTime == null || datatype == null || value == null)
                    continue;
                LocalDate d = LocalDate.parse(dateTime.substring(0, 10));

                DayAgg a = agg.computeIfAbsent(d, x -> new DayAgg());
                // NOAA GHCND values are commonly tenths in metric mode.
                // temp: tenths of °C; precip: tenths of mm.
                switch (datatype) {
                    case "TMAX" -> a.tmaxC = value / 10.0;
                    case "TMIN" -> a.tminC = value / 10.0;
                    case "PRCP" -> a.prcpMm = value / 10.0;
                }
                a.raw.add(r);
            }

            for (var e : agg.entrySet()) {
                LocalDate d = e.getKey();
                DayAgg a = e.getValue();
                String rawJson = om.writeValueAsString(a.raw);
                dailyRepo.upsertDaily(stationId, d, a.tmaxC, a.tminC, a.prcpMm, rawJson);
                rows++;
            }

            logRepo.logEvent(runId, "NOAA", "dailyGhcnd " + stationId + " " + startStr + ".." + endStr, 200, ms, null,
                    Map.of());
            pages++;

            // pagination logic from NOAA metadata
            JsonNode meta = root.path("metadata").path("resultset");
            int count = meta.path("count").asInt(-1);
            int limitReturned = meta.path("limit").asInt(limit);
            int offsetReturned = meta.path("offset").asInt(offset);

            int nextOffset = offsetReturned + limitReturned;
            if (count < 0 || nextOffset > count)
                break;
            offset = nextOffset;
        }

        if (pages > 0 || rows > 0) {
            log.info("NOAA daily chunk station={} {}..{} pages={} rows={}", stationId, start, end, pages, rows);
        } else {
            log.debug("NOAA daily chunk station={} {}..{} pages={} rows={}", stationId, start, end, pages, rows);
        }
    }

    /**
     * Safely converts a JSON number node to a {@link Double}.
     */
    private static Double getDoubleOrNull(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode())
            return null;
        return n.asDouble();
    }

    /**
     * Computes straight-line distance on Earth between two lat/lon points.
     */
    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        return R * c;
    }

    /**
     * Holds a candidate station and its distance from a gridpoint.
     */
    private static class StationPick {
        final String stationId;
        final Double distanceM;
        final Double lat;
        final Double lon;
        final Double elevation;
        final String name;

        StationPick(String stationId, Double distanceM) {
            this(stationId, distanceM, null, null, null, null);
        }

        StationPick(String stationId, Double distanceM, Double lat, Double lon, Double elevation, String name) {
            this.stationId = stationId;
            this.distanceM = distanceM;
            this.lat = lat;
            this.lon = lon;
            this.elevation = elevation;
            this.name = name;
        }
    }

    /**
     * Lightweight station record parsed from a local file.
     */
    private static record StationRecord(String id, double lat, double lon, Double elev, String name) {
    }

    /**
     * Aggregates a single day's weather values while reading NOAA rows.
     */
    private static class DayAgg {
        Double tmaxC;
        Double tminC;
        Double prcpMm;
        final List<JsonNode> raw = new ArrayList<>();
    }
}
