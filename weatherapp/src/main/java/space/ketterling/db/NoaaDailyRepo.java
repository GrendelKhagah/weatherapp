/*
* Copyright 2025 Taylor Ketterling
* NOAA Station Repository for WeatherApp, a weather data ingestion and serving application.
* Utilizes HikariCP for database connection pooling and performs upsert operations
*/
package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.time.LocalDate;

/**
 * Database access for NOAA daily summary rows.
 */
public class NoaaDailyRepo {
    private final HikariDataSource ds;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NoaaDailyRepo.class);

    /**
     * Creates a repo backed by the provided datasource.
     */
    public NoaaDailyRepo(HikariDataSource ds) {
        this.ds = ds;
    }

    /**
     * Returns the latest date stored for a station, or null if none.
     */
    public LocalDate maxDateForStation(String stationId) throws Exception {
        String sql = "SELECT MAX(date) FROM noaa_daily_summary WHERE station_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, stationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next())
                    return null;
                Date d = rs.getDate(1);
                return d == null ? null : d.toLocalDate();
            }
        }
    }

    /**
     * Inserts or updates one daily summary row for a station.
     */
    public void upsertDaily(String stationId, LocalDate date, Double tmaxC, Double tminC, Double prcpMm, String rawJson)
            throws Exception {
        String sql = "INSERT INTO noaa_daily_summary (station_id, date, tmax_c, tmin_c, prcp_mm, raw_json) " +
                "VALUES (?, ?, ?, ?, ?, ?::jsonb) " +
                "ON CONFLICT (station_id, date) DO UPDATE SET " +
                "tmax_c=EXCLUDED.tmax_c, tmin_c=EXCLUDED.tmin_c, prcp_mm=EXCLUDED.prcp_mm, raw_json=EXCLUDED.raw_json";

        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, stationId);
            ps.setDate(2, Date.valueOf(date));

            setDouble(ps, 3, tmaxC);
            setDouble(ps, 4, tminC);
            setDouble(ps, 5, prcpMm);

            ps.setString(6, rawJson);
            ps.executeUpdate();
        }

        log.debug("upsertDaily: {} {}", stationId, date);
    }

    /**
     * Writes a nullable double to a prepared statement.
     */
    private void setDouble(PreparedStatement ps, int idx, Double v) throws Exception {
        if (v == null)
            ps.setNull(idx, java.sql.Types.REAL);
        else
            ps.setDouble(idx, v);
    }
}
