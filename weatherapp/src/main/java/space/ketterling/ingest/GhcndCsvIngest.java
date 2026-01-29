package space.ketterling.ingest;

import space.ketterling.config.AppConfig;
import space.ketterling.db.Database;
import space.ketterling.db.NoaaStationRepo;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Simple CLI to ingest a local ghcnd-stations.txt into the `noaa_station`
 * table,
 * filtering to California bounding box by default.
 */
public class GhcndCsvIngest {
    private static final Logger log = LoggerFactory.getLogger(GhcndCsvIngest.class);

    public static void main(String[] args) throws Exception {
        String path = null;
        if (args.length > 0)
            path = args[0];

        AppConfig cfg = AppConfig.load();
        HikariDataSource ds = Database.createIngestDataSource(cfg);
        NoaaStationRepo repo = new NoaaStationRepo(ds);

        log.info("Starting GhcndCsvIngest path={}", path == null ? "resource" : path);
        BufferedReader br;
        if (path == null) {
            var is = GhcndCsvIngest.class.getClassLoader().getResourceAsStream("noaaData/ghcnd-stations.txt");
            if (is == null) {
                log.error("No ghcnd-stations.txt found in resources and no path provided");
                return;
            }
            br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        } else {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
        }

        int count = 0;
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isBlank())
                continue;
            String[] parts = line.trim().split("\\s+", 5);
            if (parts.length < 3)
                continue;
            String id = parts[0];
            double lat = Double.parseDouble(parts[1]);
            double lon = Double.parseDouble(parts[2]);
            Double elev = null;
            if (parts.length >= 4) {
                try {
                    elev = Double.parseDouble(parts[3]);
                } catch (Exception ignored) {
                }
            }
            String name = parts.length >= 5 ? parts[4].trim() : null;

            // California bounding box filter
            if (!(lat >= 32.5 && lat <= 42.0 && lon >= -124.5 && lon <= -114.0))
                continue;

            repo.upsertStation(id.toUpperCase(Locale.ROOT), name, lat, lon, elev, "{\"source\":\"local-ghcnd\"}");
            count++;
        }

        log.info("Imported {} CA stations into noaa_station", count);
        ds.close();
    }
}
