package space.ketterling.ingest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import space.ketterling.config.AppConfig;
import space.ketterling.db.Database;
import space.ketterling.db.NoaaDailyRepo;
import space.ketterling.db.NoaaStationRepo;

/**
 * Ingest local station historic CSV files into noaa_daily_summary.
 *
 * Supports "wide" NOAA-style daily CSVs:
 * "STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","PRCP",...,"TMAX","TMIN"
 *
 * Also leaves room for long-format files (not implemented here).
 */
public class StationHistoricCsvIngest {
    private static final Logger log = LoggerFactory.getLogger(StationHistoricCsvIngest.class);
    private static volatile boolean loggedReadOnlyBase = false;

    /**
     * CLI entry point for ingesting local station history files.
     *
     * <p>
     * Optionally pass a directory path; otherwise it uses config and defaults.
     * </p>
     */
    public static void main(String[] args) throws Exception {
        String dirArg = (args.length > 0) ? args[0] : null;

        AppConfig cfg = AppConfig.load();
        HikariDataSource ds = Database.createIngestDataSource(cfg);

        Path dir = resolveStationHistoricDir(dirArg, cfg);

        if (dir == null) {
            log.error("No stationHistoricData directory found (provide path as first arg or set STATION_HISTORIC_DIR)");
            ds.close();
            return;
        }

        int processed = ingestIfNeeded(dir, ds, cfg);
        int tarRows = ingestDailySummariesTarGzIfPresent(dir, ds, cfg);
        log.info("Station historic ingest completed, files processed={} tarRows={}", processed, tarRows);
        ds.close();
    }

    /**
     * Finds the directory that holds station history CSV files.
     */
    private static Path resolveStationHistoricDir(String dirArg, AppConfig cfg) {
        // 1) explicit CLI arg
        if (dirArg != null) {
            Path p = Path.of(dirArg);
            if (Files.exists(p) && Files.isDirectory(p))
                return p;
        }

        // 2) config path (best for systemd)
        try {
            String cfgDir = cfg.stationHistoricDir();
            if (cfgDir != null && !cfgDir.isBlank()) {
                Path p = Path.of(cfgDir);
                if (Files.exists(p) && Files.isDirectory(p))
                    return p;
            }
        } catch (Exception ignored) {
        }

        // 3) dev / local fallbacks
        Path p1 = Path.of("target/classes/stationHistoricData");
        if (Files.exists(p1) && Files.isDirectory(p1))
            return p1;

        Path p2 = Path.of("src/main/resources/stationHistoricData");
        if (Files.exists(p2) && Files.isDirectory(p2))
            return p2;

        return null;
    }

    /**
     * Scans a directory for station CSVs and ingests only new rows.
     *
     * <p>
     * Uses a small state file to skip files that have not changed.
     * </p>
     *
     * @return number of files that wrote at least one row
     */
    public static int ingestIfNeeded(Path dir, HikariDataSource ds, AppConfig cfg) throws Exception {
        NoaaDailyRepo dailyRepo = new NoaaDailyRepo(ds);
        NoaaStationRepo stationRepo = new NoaaStationRepo(ds);
        ObjectMapper om = new ObjectMapper();

        int filesProcessed = 0;
        int rowsWritten = 0;

        // persistent state to avoid re-scanning unchanged files
        Path stateFile = Path.of(cfg.stationHistoricStateFile());
        Properties state = loadState(stateFile);
        Map<String, Long> lastSeen = new HashMap<>();

        for (String k : state.stringPropertyNames()) {
            try {
                lastSeen.put(k, Long.parseLong(state.getProperty(k)));
            } catch (Exception ignored) {
            }
        }

        try (Stream<Path> stream = Files.list(dir)) {
            for (Path p : stream.toList()) {
                if (!Files.isRegularFile(p))
                    continue;
                String fileName = p.getFileName().toString();
                if (!fileName.toLowerCase(Locale.ROOT).endsWith(".csv"))
                    continue;

                // This is only for state tracking (filename-based)
                String stationRawFromFilename = fileName.substring(0, fileName.lastIndexOf('.'));
                String stateStationId = stationRawFromFilename.startsWith("GHCND:")
                        ? stationRawFromFilename
                        : "GHCND:" + stationRawFromFilename;

                long mod = Files.getLastModifiedTime(p).toMillis();
                long seen = lastSeen.getOrDefault(stateStationId, 0L);
                boolean skipIngest = mod <= seen;
                if (skipIngest) {
                    log.debug("Skipping ingest for {}: unchanged since last ingest", fileName);
                }

                // Use filename-based id to query dbMax as a starting point.
                // Note: actual rows may contain a STATION column that matches this.
                LocalDate dbMax = skipIngest ? null : dailyRepo.maxDateForStation(stateStationId);
                // For wide files we write rows directly; keep some counters
                boolean wideFile = false;
                int wideRowsWritten = 0;
                LocalDate fileMax = null;

                // For non-wide (long format), you can implement aggregation later if needed
                Map<LocalDate, DayAgg> agg = new HashMap<>();

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(p.toFile()), StandardCharsets.UTF_8))) {

                    String headerLine = br.readLine();
                    if (headerLine == null) {
                        log.debug("Skipping {}: empty file", fileName);
                        lastSeen.put(stateStationId, mod);
                        continue;
                    }

                    var headerRaw = parseCsvLine(headerLine);
                    var header = new ArrayList<String>(headerRaw.size());
                    for (String h : headerRaw)
                        header.add(h.trim().toUpperCase(Locale.ROOT));

                    boolean wide = header.contains("STATION") && header.contains("DATE") &&
                            (header.contains("PRCP") || header.contains("TMAX") || header.contains("TMIN"));

                    Map<String, Integer> idx = new HashMap<>();
                    for (int i = 0; i < header.size(); i++)
                        idx.put(header.get(i), i);

                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isBlank())
                            continue;

                        var cols = parseCsvLine(line);

                        if (wide) {
                            wideFile = true;

                            // guard indexes
                            Integer stI = idx.get("STATION");
                            Integer dtI = idx.get("DATE");
                            if (stI == null || dtI == null)
                                continue;
                            if (stI >= cols.size() || dtI >= cols.size())
                                continue;

                            String stationRawId = cols.get(stI);
                            String dateStr = cols.get(dtI);

                            LocalDate d;
                            try {
                                d = LocalDate.parse(dateStr.trim());
                            } catch (Exception ex) {
                                continue;
                            }

                            if (fileMax == null || d.isAfter(fileMax))
                                fileMax = d;

                            // build station id from the row (this is what you write to DB)
                            String rowStationId = (stationRawId != null && stationRawId.startsWith("GHCND:"))
                                    ? stationRawId
                                    : "GHCND:" + stationRawId;

                            // only ingest rows newer than dbMax
                            if (skipIngest)
                                continue;
                            if (dbMax != null && !d.isAfter(dbMax)) {
                                continue;
                            }

                            String latStr = getCol(cols, idx, "LATITUDE");
                            String lonStr = getCol(cols, idx, "LONGITUDE");
                            String elevStr = getCol(cols, idx, "ELEVATION");
                            String name = getCol(cols, idx, "NAME");
                            if (name == null || name.isBlank())
                                name = stationRawId;

                            Double prcp = parseMaybeNumber(getCol(cols, idx, "PRCP"));
                            Double tmax = parseMaybeNumber(getCol(cols, idx, "TMAX"));
                            Double tmin = parseMaybeNumber(getCol(cols, idx, "TMIN"));

                            // NOAA daily CSVs are typically tenths (mm*10, C*10)
                            Double prcpMm = (prcp == null) ? null : prcp / 10.0;
                            Double tmaxC = (tmax == null) ? null : tmax / 10.0;
                            Double tminC = (tmin == null) ? null : tmin / 10.0;

                            Double lat = parseMaybeNumber(latStr);
                            Double lon = parseMaybeNumber(lonStr);
                            Double elev = parseMaybeNumber(elevStr);

                            // upsert station + daily record
                            stationRepo.upsertStation(rowStationId, name, lat, lon, elev, "{\"source\":\"local-csv\"}");
                            dailyRepo.upsertDaily(rowStationId, d, tmaxC, tminC, prcpMm, "{}");

                            wideRowsWritten++;
                            rowsWritten++;
                            continue;
                        }

                        // TODO: long-format files can be supported here if you still want them.
                        // For now, no-op.
                    }
                }

                // finalize per-file
                if (wideFile) {
                    if (wideRowsWritten == 0) {
                        log.debug("No new rows in {} (fileMax={} dbMax={}); marking as seen", fileName, fileMax, dbMax);
                    } else {
                        filesProcessed++;
                        log.info("Ingested file {} -> newRows={}", fileName, wideRowsWritten);
                    }
                    lastSeen.put(stateStationId, mod);
                    if (fileMax != null) {
                        moveFileToDateDir(dir, p, fileMax);
                    }
                    continue;
                }

                // If not wide and no long-format logic, just mark seen so we don’t loop forever
                if (fileMax == null && agg.isEmpty()) {
                    log.debug("Skipping {}: unsupported format or no parsable dates; marking as seen", fileName);
                    lastSeen.put(stateStationId, mod);
                    continue;
                }

                // If you later implement long-format agg, keep your old logic here.
                if (agg.isEmpty()) {
                    log.debug("No new rows in {} (fileMax={} dbMax={}); marking as seen", fileName, fileMax, dbMax);
                    lastSeen.put(stateStationId, mod);
                    continue;
                }

                log.info("Ingesting local CSV {} for station {} (fileMax={} dbMax={})",
                        fileName, stateStationId, fileMax, dbMax);

                stationRepo.upsertStation(stateStationId, stationRawFromFilename, null, null, null,
                        "{\"source\":\"local-csv\"}");

                filesProcessed++;
                for (var e : agg.entrySet()) {
                    LocalDate d = e.getKey();
                    DayAgg a = e.getValue();
                    String rawJson = om.writeValueAsString(a.raw);
                    dailyRepo.upsertDaily(stateStationId, d, a.tmaxC, a.tminC, a.prcpMm, rawJson);
                    rowsWritten++;
                }

                log.info("Ingested file {} -> station={} newRows={}", fileName, stateStationId, agg.size());
                lastSeen.put(stateStationId, mod);
                if (fileMax != null) {
                    moveFileToDateDir(dir, p, fileMax);
                }
            }
        }

        // persist state
        for (var e : lastSeen.entrySet())
            state.setProperty(e.getKey(), Long.toString(e.getValue()));
        saveState(stateFile, state, "station historic ingest state: stationId -> lastModifiedMillis");

        log.info("Completed ingest: filesProcessed={} rowsInsertedOrUpdated={}", filesProcessed, rowsWritten);
        return filesProcessed;
    }

    /**
     * Scans for daily-summaries-latest.tar.gz and ingests all CSV entries.
     *
     * <p>
     * Only imports rows newer than the DB's max date per station. After ingest,
     * the archive is moved into an oldDailys/ folder.
     * </p>
     *
     * @return total rows written
     */
    public static int ingestDailySummariesTarGzIfPresent(Path stationDir, HikariDataSource ds, AppConfig cfg)
            throws Exception {
        Path tarPath = resolveDailySummariesTarGz(stationDir, cfg);
        if (tarPath == null || !Files.isRegularFile(tarPath)) {
            return 0;
        }

        long mod = Files.getLastModifiedTime(tarPath).toMillis();
        Path stateFile = Path.of(cfg.stationHistoricStateFile());
        Properties state = loadState(stateFile);
        String tarKey = "dailySummariesTar.lastModified";
        long seen = parseLongOrZero(state.getProperty(tarKey));
        if (mod <= seen) {
            log.info("Skipping daily summaries tar.gz: unchanged since last ingest");
            return 0;
        }

        NoaaDailyRepo dailyRepo = new NoaaDailyRepo(ds);
        NoaaStationRepo stationRepo = new NoaaStationRepo(ds);

        log.info("Ingesting daily summaries archive {}", tarPath);

        Map<String, LocalDate> dbMaxCache = new HashMap<>();
        int rowsWritten = 0;
        int filesProcessed = 0;

        try (var fis = Files.newInputStream(tarPath);
                var bis = new BufferedInputStream(fis);
                var gis = new GzipCompressorInputStream(bis);
                var tis = new TarArchiveInputStream(gis)) {

            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
                if (entry.isDirectory())
                    continue;
                String entryName = entry.getName();
                if (entryName == null)
                    continue;
                String entryLower = entryName.toLowerCase(Locale.ROOT);
                if (!entryLower.endsWith(".csv"))
                    continue;

                String fileName = Path.of(entryName).getFileName().toString();

                BufferedReader br = new BufferedReader(new InputStreamReader(tis, StandardCharsets.UTF_8));
                String headerLine = br.readLine();
                if (headerLine == null) {
                    continue;
                }

                var headerRaw = parseCsvLine(headerLine);
                var header = new ArrayList<String>(headerRaw.size());
                for (String h : headerRaw)
                    header.add(h.trim().toUpperCase(Locale.ROOT));

                boolean wide = header.contains("STATION") && header.contains("DATE") &&
                        (header.contains("PRCP") || header.contains("TMAX") || header.contains("TMIN"));

                if (!wide) {
                    log.debug("Skipping archive entry {}: unsupported header", fileName);
                    continue;
                }

                Map<String, Integer> idx = new HashMap<>();
                for (int i = 0; i < header.size(); i++)
                    idx.put(header.get(i), i);

                int entryRowsWritten = 0;

                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isBlank())
                        continue;
                    var cols = parseCsvLine(line);

                    Integer stI = idx.get("STATION");
                    Integer dtI = idx.get("DATE");
                    if (stI == null || dtI == null)
                        continue;
                    if (stI >= cols.size() || dtI >= cols.size())
                        continue;

                    String stationRawId = cols.get(stI);
                    String dateStr = cols.get(dtI);
                    LocalDate d;
                    try {
                        d = LocalDate.parse(dateStr.trim());
                    } catch (Exception ex) {
                        continue;
                    }

                    String rowStationId = (stationRawId != null && stationRawId.startsWith("GHCND:"))
                            ? stationRawId
                            : "GHCND:" + stationRawId;

                    LocalDate dbMax = dbMaxCache.computeIfAbsent(rowStationId, id -> {
                        try {
                            return dailyRepo.maxDateForStation(id);
                        } catch (Exception e) {
                            return null;
                        }
                    });

                    if (dbMax != null && !d.isAfter(dbMax)) {
                        continue;
                    }

                    String latStr = getCol(cols, idx, "LATITUDE");
                    String lonStr = getCol(cols, idx, "LONGITUDE");
                    String elevStr = getCol(cols, idx, "ELEVATION");
                    String name = getCol(cols, idx, "NAME");
                    if (name == null || name.isBlank())
                        name = stationRawId;

                    Double prcp = parseMaybeNumber(getCol(cols, idx, "PRCP"));
                    Double tmax = parseMaybeNumber(getCol(cols, idx, "TMAX"));
                    Double tmin = parseMaybeNumber(getCol(cols, idx, "TMIN"));

                    Double prcpMm = (prcp == null) ? null : prcp / 10.0;
                    Double tmaxC = (tmax == null) ? null : tmax / 10.0;
                    Double tminC = (tmin == null) ? null : tmin / 10.0;

                    Double lat = parseMaybeNumber(latStr);
                    Double lon = parseMaybeNumber(lonStr);
                    Double elev = parseMaybeNumber(elevStr);

                    stationRepo.upsertStation(rowStationId, name, lat, lon, elev,
                            "{\"source\":\"daily-summaries-tar\"}");
                    dailyRepo.upsertDaily(rowStationId, d, tmaxC, tminC, prcpMm, "{}");

                    dbMaxCache.put(rowStationId, d);
                    entryRowsWritten++;
                    rowsWritten++;
                }

                if (entryRowsWritten > 0) {
                    filesProcessed++;
                    log.info("Ingested archive entry {} -> newRows={}", fileName, entryRowsWritten);
                } else {
                    log.debug("No new rows in archive entry {}", fileName);
                }
            }
        }

        state.setProperty(tarKey, Long.toString(mod));
        saveState(stateFile, state, "station historic ingest state (including tar.gz)");

        moveTarToOld(tarPath);

        log.info("Completed daily summaries ingest: filesProcessed={} rowsInsertedOrUpdated={}", filesProcessed,
                rowsWritten);
        return rowsWritten;
    }

    /**
     * Searches common locations for the daily summaries archive.
     */
    private static Path resolveDailySummariesTarGz(Path stationDir, AppConfig cfg) {
        if (stationDir != null) {
            Path p1 = stationDir.resolve("daily-summaries-latest.tar.gz");
            if (Files.exists(p1))
                return p1;
            Path parent = stationDir.getParent();
            if (parent != null) {
                Path p2 = parent.resolve("daily-summaries-latest.tar.gz");
                if (Files.exists(p2))
                    return p2;
            }
        }

        try {
            Path cfgDir = Path.of(cfg.stationHistoricDir());
            Path p3 = cfgDir.resolve("daily-summaries-latest.tar.gz");
            if (Files.exists(p3))
                return p3;
            Path parent = cfgDir.getParent();
            if (parent != null) {
                Path p4 = parent.resolve("daily-summaries-latest.tar.gz");
                if (Files.exists(p4))
                    return p4;
            }
        } catch (Exception ignored) {
        }

        Path p5 = Path.of("/opt/weather-app/data/daily-summaries-latest.tar.gz");
        if (Files.exists(p5))
            return p5;

        Path p6 = Path.of("src/main/resources/daily-summaries-latest.tar.gz");
        if (Files.exists(p6))
            return p6;

        Path p7 = Path.of("src/main/resources/stationHistoricData/daily-summaries-latest.tar.gz");
        if (Files.exists(p7))
            return p7;

        Path p8 = Path.of("target/classes/daily-summaries-latest.tar.gz");
        if (Files.exists(p8))
            return p8;

        Path p9 = Path.of("target/classes/stationHistoricData/daily-summaries-latest.tar.gz");
        if (Files.exists(p9))
            return p9;

        return null;
    }

    /**
     * Moves the processed tar.gz into an oldDailys/ folder for safekeeping.
     */
    private static void moveTarToOld(Path tarPath) {
        try {
            Path parent = tarPath.getParent();
            Path base = parent == null ? Path.of(".") : parent;
            if (!canWrite(base)) {
                warnReadOnlyOnce(base);
                return;
            }
            Path oldDir = base.resolve("oldDailys");
            Files.createDirectories(oldDir);
            Path target = oldDir.resolve(tarPath.getFileName());
            Files.move(tarPath, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved daily summaries archive to {}", target);
        } catch (Exception e) {
            log.warn("Failed to move daily summaries archive {}: {}", tarPath, e.getMessage());
        }
    }

    /**
     * Moves a CSV file into a date-named folder based on the max date found.
     */
    private static void moveFileToDateDir(Path baseDir, Path file, LocalDate fileMax) {
        try {
            if (!canWrite(baseDir)) {
                warnReadOnlyOnce(baseDir);
                return;
            }
            Path dateDir = baseDir.resolve(fileMax.toString());
            Files.createDirectories(dateDir);
            Path target = dateDir.resolve(file.getFileName());
            Files.move(file, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved {} to {}", file.getFileName(), dateDir);
        } catch (Exception e) {
            log.warn("Failed to move {} to date dir {}: {}", file.getFileName(), fileMax, e.getMessage());
        }
    }

    /**
     * Returns true if the directory can be written to.
     */
    private static boolean canWrite(Path dir) {
        try {
            return Files.isWritable(dir);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Logs a one-time warning when the base directory is read-only.
     */
    private static void warnReadOnlyOnce(Path dir) {
        if (!loggedReadOnlyBase) {
            loggedReadOnlyBase = true;
            log.warn("Base directory is read-only; skipping file moves: {}", dir);
        }
    }

    /**
     * Loads a simple key/value state file used for ingest bookkeeping.
     */
    private static Properties loadState(Path stateFile) {
        Properties state = new Properties();
        if (Files.exists(stateFile)) {
            try (FileInputStream fis = new FileInputStream(stateFile.toFile())) {
                state.load(fis);
            } catch (IOException ignored) {
            }
        }
        return state;
    }

    /**
     * Saves the ingest state file to disk.
     */
    private static void saveState(Path stateFile, Properties state, String comment) {
        try (FileOutputStream fos = new FileOutputStream(stateFile.toFile())) {
            state.store(fos, comment);
        } catch (IOException ignored) {
        }
    }

    /**
     * Parses a string to long or returns 0 if missing/invalid.
     */
    private static long parseLongOrZero(String s) {
        if (s == null || s.isBlank())
            return 0L;
        try {
            return Long.parseLong(s.trim());
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * Gets a CSV column by header name (uppercase keys).
     */
    private static String getCol(java.util.List<String> cols, Map<String, Integer> idx, String keyUpper) {
        Integer i = idx.get(keyUpper);
        if (i == null)
            return null;
        if (i < 0 || i >= cols.size())
            return null;
        return cols.get(i);
    }

    /**
     * Holds a single day's aggregated values while parsing long-format files.
     */
    private static class DayAgg {
        Double tmaxC;
        Double tminC;
        Double prcpMm;
        final java.util.List<String> raw = new java.util.ArrayList<>();
    }

    /**
     * Splits a CSV line while handling quoted commas.
     */
    private static java.util.List<String> parseCsvLine(String line) {
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString().trim());
        // strip outer quotes
        for (int i = 0; i < out.size(); i++) {
            String s = out.get(i);
            if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
                s = s.substring(1, s.length() - 1);
            }
            out.set(i, s);
        }
        return out;
    }

    /**
     * Parses a double, returning null if the value is missing or invalid.
     */
    private static Double parseMaybeNumber(String s) {
        if (s == null)
            return null;
        s = s.trim();
        if (s.isEmpty())
            return null;
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return null;
        }
    }
}
