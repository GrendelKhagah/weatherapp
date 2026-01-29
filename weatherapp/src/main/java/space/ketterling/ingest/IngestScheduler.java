package space.ketterling.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import space.ketterling.config.AppConfig;

import java.util.concurrent.*;
import org.slf4j.MDC;

public final class IngestScheduler {
    private static final Logger log = LoggerFactory.getLogger(IngestScheduler.class);

    // Executors for each job type, single-threaded
    private final ScheduledExecutorService gridExec = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "ingest-gridpoints"));
    private final ScheduledExecutorService hourlyExec = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "ingest-hourly"));
    private final ScheduledExecutorService alertsExec = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "ingest-alerts"));

    private final AppConfig cfg;
    private final NwsIngestService ingest;
    private final space.ketterling.ingest.NoaaIngestService noaaIngest; // nullable

    private ScheduledFuture<?> gridpointTask;
    private ScheduledFuture<?> hourlyTask;
    private ScheduledFuture<?> alertsTask;

    private final ScheduledExecutorService noaaStationsExec = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "ingest-noaa-stations"));
    private final ScheduledExecutorService noaaDailyExec = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "ingest-noaa-daily"));
    private final ScheduledExecutorService noaaCacheExec = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "ingest-noaa-cache"));
    private ScheduledFuture<?> noaaStationsTask;
    private ScheduledFuture<?> noaaDailyTask;
    private ScheduledFuture<?> noaaCacheTask;

    public IngestScheduler(AppConfig cfg, NwsIngestService ingest,
            space.ketterling.ingest.NoaaIngestService noaaIngest) {
        this.cfg = cfg;
        this.ingest = ingest;
        this.noaaIngest = noaaIngest;
    }

    public void start() {
        gridpointTask = gridExec.scheduleWithFixedDelay(safe("gridpoints", ingest::refreshGridpoints),
                0, cfg.gridpointRefresh().toSeconds(), TimeUnit.SECONDS);

        hourlyTask = hourlyExec.scheduleWithFixedDelay(safe("hourlyForecast", ingest::ingestHourlyForecasts),
                30, cfg.hourlyForecast().toSeconds(), TimeUnit.SECONDS);

        alertsTask = alertsExec.scheduleWithFixedDelay(safe("alerts", ingest::ingestAlerts),
                10, cfg.alerts().toSeconds(), TimeUnit.SECONDS);

        // NOAA jobs (if provided and enabled)
        if (noaaIngest != null && cfg.noaaApiEnabled()) {
            noaaStationsTask = noaaStationsExec.scheduleWithFixedDelay(
                    safe("noaaStations", noaaIngest::refreshStationsAndMapping),
                    5, cfg.noaaStationsRefresh().toSeconds(), TimeUnit.SECONDS);

            noaaDailyTask = noaaDailyExec.scheduleWithFixedDelay(safe("noaaDaily", noaaIngest::ingestDailyHistory),
                    15, cfg.noaaDailyHistory().toSeconds(), TimeUnit.SECONDS);

            // cache population runs shortly after daily history job (same cadence)
            noaaCacheTask = noaaCacheExec.scheduleWithFixedDelay(safe("noaaCache", () -> {
                try {
                    java.time.LocalDate asOf = java.time.LocalDate.now(cfg.clockZoneId()).minusDays(1);
                    noaaIngest.populateCache(asOf, 30);
                } catch (Exception e) {
                    throw e;
                }
            }), 30, cfg.noaaDailyHistory().toSeconds(), TimeUnit.SECONDS);
        }

        log.info("Ingest scheduler started.");
    }

    public void stop() {
        if (gridpointTask != null)
            gridpointTask.cancel(true);
        if (hourlyTask != null)
            hourlyTask.cancel(true);
        if (alertsTask != null)
            alertsTask.cancel(true);

        shutdown(gridExec, "gridExec");
        shutdown(hourlyExec, "hourlyExec");
        shutdown(alertsExec, "alertsExec");

        if (noaaIngest != null) {
            if (noaaStationsTask != null)
                noaaStationsTask.cancel(true);
            if (noaaDailyTask != null)
                noaaDailyTask.cancel(true);
            if (noaaCacheTask != null)
                noaaCacheTask.cancel(true);
            shutdown(noaaStationsExec, "noaaStationsExec");
            shutdown(noaaDailyExec, "noaaDailyExec");
            shutdown(noaaCacheExec, "noaaCacheExec");
        }
    }

    private void shutdown(ScheduledExecutorService es, String name) {
        es.shutdownNow();
        try {
            if (!es.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("{} did not terminate cleanly", name);
            }
        } catch (InterruptedException ignored) {
        }
    }

    private Runnable safe(String name, ThrowingRunnable r) {
        return () -> {
            MDC.put("job", name);
            try {
                r.run();
            } catch (Exception e) {
                log.error("Scheduled job failed: {}", name, e);
            } finally {
                MDC.remove("job");
            }
        };
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
