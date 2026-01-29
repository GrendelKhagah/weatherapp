package space.ketterling.db;

import space.ketterling.config.AppConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class Database {
    public static HikariDataSource createApiDataSource(AppConfig cfg) {
        return createDataSource(cfg, "api", cfg.dbApiPoolMax());
    }

    public static HikariDataSource createIngestDataSource(AppConfig cfg) {
        return createDataSource(cfg, "ingest", cfg.dbIngestPoolMax());
    }

    private static HikariDataSource createDataSource(AppConfig cfg, String role, int maxPool) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.dbJdbcUrl());
        hc.setUsername(cfg.dbUsername());
        hc.setPassword(cfg.dbPassword());
        hc.setPoolName("weatherapp-" + role);
        hc.setMaximumPoolSize(Math.max(2, maxPool));
        hc.setMinimumIdle(1);
        hc.setConnectionTimeout(10_000);
        hc.setLeakDetectionThreshold(0); // enable later if you suspect leaks
        return new HikariDataSource(hc);
    }
}
