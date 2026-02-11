package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Database access for hourly forecast rows.
 */
public class ForecastRepo {
    private final HikariDataSource ds;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ForecastRepo.class);

    /**
     * Creates a repo backed by the provided datasource.
     */
    public ForecastRepo(HikariDataSource ds) {
        this.ds = ds;
    }

    /**
     * Inserts or updates a single hourly forecast row.
     */
    public void upsertHourly(
            String gridId,
            Instant startTime,
            Instant endTime,
            Double temperatureC,
            Double windSpeedMps,
            Double windGustMps,
            Double windDirDeg,
            Double precipProb,
            Double relativeHumidity,
            String shortForecast,
            Instant issuedAt,
            String rawJson) throws Exception {

        String sql = "INSERT INTO nws_forecast_hourly (" +
                "grid_id, start_time, end_time, temperature_c, wind_speed_mps, wind_gust_mps, wind_dir_deg, " +
                "precip_prob, relative_humidity, short_forecast, issued_at, raw_json, ingested_at" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, now()) " +
                "ON CONFLICT (grid_id, start_time) DO UPDATE SET " +
                "end_time=EXCLUDED.end_time, " +
                "temperature_c=EXCLUDED.temperature_c, wind_speed_mps=EXCLUDED.wind_speed_mps, wind_gust_mps=EXCLUDED.wind_gust_mps, "
                +
                "wind_dir_deg=EXCLUDED.wind_dir_deg, precip_prob=EXCLUDED.precip_prob, relative_humidity=EXCLUDED.relative_humidity, "
                +
                "short_forecast=EXCLUDED.short_forecast, issued_at=EXCLUDED.issued_at, raw_json=EXCLUDED.raw_json, ingested_at=now()";

        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setString(1, gridId);
            ps.setTimestamp(2, Timestamp.from(startTime));
            ps.setTimestamp(3, Timestamp.from(endTime));

            setDouble(ps, 4, temperatureC);
            setDouble(ps, 5, windSpeedMps);
            setDouble(ps, 6, windGustMps);
            setDouble(ps, 7, windDirDeg);
            setDouble(ps, 8, precipProb);
            setDouble(ps, 9, relativeHumidity);

            ps.setString(10, shortForecast);

            if (issuedAt == null)
                ps.setNull(11, java.sql.Types.TIMESTAMP);
            else
                ps.setTimestamp(11, Timestamp.from(issuedAt));

            ps.setString(12, rawJson);
            ps.executeUpdate();
        }
        log.debug("upsertHourly: gridId={} start={} end={}", gridId, startTime, endTime);
    }

    /**
     * Writes a nullable double to a prepared statement.
     */
    private void setDouble(PreparedStatement ps, int idx, Double v) throws Exception {
        if (v == null)
            ps.setNull(idx, java.sql.Types.DOUBLE);
        else
            ps.setDouble(idx, v);
    }

}
