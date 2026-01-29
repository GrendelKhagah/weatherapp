/*
* Copyright 2025 Taylor Ketterling
* Main application entry point for WeatherApp, a weather data ingestion and serving application.
*
* Initializes configuration, database connections, HTTP clients, ingestion services,
* and starts the API server. This class orchestrates the overall API and ingestion workflow.
* Startup flow includes loading configuration, setting up repositories, clients, services,
* scheduler, initializes API server, and historical data ingestion along with NOAA API ingestion; 
* program also handles a graceful shutdown.
*/

package space.ketterling;

import space.ketterling.api.ApiServer;
import space.ketterling.config.AppConfig;
import space.ketterling.db.*;
import space.ketterling.ingest.*;
import space.ketterling.noaa.NoaaClient;
import space.ketterling.nws.NwsClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("Starting application");
        AppConfig cfg = AppConfig.load();

        ObjectMapper om = new ObjectMapper();
        HikariDataSource apiDs = Database.createApiDataSource(cfg);
        HikariDataSource ingestDs = Database.createIngestDataSource(cfg);

        // Repos
        IngestLogRepo ingestLog = new IngestLogRepo(ingestDs, om);
        GridpointRepo gridRepo = new GridpointRepo(ingestDs);
        ForecastRepo forecastRepo = new ForecastRepo(ingestDs);
        AlertRepo alertRepo = new AlertRepo(ingestDs);
        TrackedPointRepo trackedPointRepo = new TrackedPointRepo(ingestDs);
        try {
            if (trackedPointRepo.list().isEmpty()) {
                int i = 1;
                for (double[] pt : cfg.trackedPoints()) {
                    trackedPointRepo.upsert("Config Point " + i, pt[0], pt[1]);
                    i++;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to seed tracked points from config", e);
        }

        // HTTP client (NWS)
        NwsClient nwsClient = new NwsClient(cfg, om);

        // Ingest services
        NwsIngestService ingest = new NwsIngestService(cfg, om, nwsClient, ingestLog, gridRepo, forecastRepo,
                alertRepo, trackedPointRepo);

        // NOAA client + repos + service
        final NoaaIngestService noaaIngest;
        if (cfg.noaaApiEnabled()) {
            NoaaClient noaaClient = new NoaaClient(cfg, om);
            NoaaStationRepo noaaStationRepo = new NoaaStationRepo(ingestDs);
            NoaaDailyRepo noaaDailyRepo = new NoaaDailyRepo(ingestDs);
            GridpointStationMapRepo mapRepo = new GridpointStationMapRepo(ingestDs);
            noaaIngest = new NoaaIngestService(cfg, om, noaaClient, ingestLog, noaaStationRepo,
                    noaaDailyRepo, mapRepo, gridRepo);
        } else {
            log.info("NOAA API ingest disabled by config");
            noaaIngest = null;
        }

        // Scheduler
        IngestScheduler scheduler = new IngestScheduler(cfg, ingest, noaaIngest);
        scheduler.start();

        // API server (start immediately; ingestion runs in background)
        ApiServer api = new ApiServer(cfg, om, apiDs, () -> {
            try {
                ingest.refreshGridpoints();
            } catch (Exception e) {
                log.error("Manual gridpoint refresh failed", e);
            }
        }, nwsClient);
        api.start();
        log.info("API server started on port {}", cfg.apiPort());

        // Run initial refreshes on startup (so that data exists) after API launches
        Thread startupIngest = new Thread(() -> {
            try {
                org.slf4j.MDC.put("job", "startup-gridpoints");
                ingest.refreshGridpoints();
                org.slf4j.MDC.remove("job");

                // If local station historic CSVs exist, ingest any new local data first
                Path stationDir = Path.of(cfg.stationHistoricDir());
                if (!Files.exists(stationDir))
                    stationDir = Path.of("/opt/weather-app/data/stationHistoricData");
                if (Files.exists(stationDir)) {
                    try {
                        org.slf4j.MDC.put("job", "startup-local-historic");
                        log.info("Local stationHistoricData directory found, checking for new CSVs");
                        int processed = space.ketterling.ingest.StationHistoricCsvIngest.ingestIfNeeded(stationDir,
                                ingestDs,
                                cfg);
                        int tarRows = space.ketterling.ingest.StationHistoricCsvIngest
                                .ingestDailySummariesTarGzIfPresent(stationDir, ingestDs, cfg);
                        log.info("Local historic ingest processed {} files, tarRows={}", processed, tarRows);
                    } catch (Exception e) {
                        log.error("Error ingesting local station historic CSVs", e);
                    } finally {
                        org.slf4j.MDC.remove("job");
                    }
                }

                // Initial hourly forecast ingest from noaa if the api is enabled in cfg
                if (cfg.noaaApiEnabled() && noaaIngest != null) {
                    org.slf4j.MDC.put("job", "startup-noaa-mapping");
                    noaaIngest.refreshStationsAndMapping();
                    org.slf4j.MDC.remove("job");
                }

                // Continue with NOAA API backfill for any remaining history
                if (cfg.noaaApiEnabled() && noaaIngest != null) {
                    org.slf4j.MDC.put("job", "startup-noaa-daily");
                    noaaIngest.ingestDailyHistory();
                    org.slf4j.MDC.remove("job");
                }
            } catch (Exception e) {
                log.error("Startup ingest failed", e);
                org.slf4j.MDC.remove("job");
            }
        }, "startup-ingest");
        startupIngest.setDaemon(true);
        startupIngest.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Shutting down...");
                api.stop();
                scheduler.stop();
                apiDs.close();
                ingestDs.close();
            } catch (Exception e) {
                log.error("Shutdown error", e);
            }
        }));
    }
}
