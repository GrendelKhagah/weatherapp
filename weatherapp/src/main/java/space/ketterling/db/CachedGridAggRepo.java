package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class CachedGridAggRepo {
    private final HikariDataSource ds;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CachedGridAggRepo.class);

    public CachedGridAggRepo(HikariDataSource ds) {
        this.ds = ds;
    }

    /**
     * Upsert aggregate rows for a given asOf date and precipitation window (days).
     */
    public void upsertAggregates(LocalDate asOf, int prcpWindowDays) throws Exception {
        String sql = "INSERT INTO cached_grid_agg (grid_id, as_of, tmean_c, prcp_30d_mm, last_updated) "
                + "SELECT g.grid_id, ?::date AS as_of, "
                + "AVG((d.tmax_c + d.tmin_c)/2.0) AS tmean_c, "
                + "SUM(d.prcp_mm) AS prcp_30d_mm, now() "
                + "FROM geo_gridpoint g "
                + "JOIN gridpoint_station_map m ON g.grid_id = m.grid_id AND m.is_primary = true "
                + "JOIN noaa_daily_summary d ON d.station_id = m.station_id "
                + "  AND d.date > (?::date - (? * INTERVAL '1 day')) AND d.date <= ?::date "
                + "GROUP BY g.grid_id "
                + "ON CONFLICT (grid_id) DO UPDATE SET as_of = EXCLUDED.as_of, tmean_c = EXCLUDED.tmean_c, prcp_30d_mm = EXCLUDED.prcp_30d_mm, last_updated = now()";

        String countSql = "SELECT COUNT(DISTINCT g.grid_id) FROM geo_gridpoint g "
                + "JOIN gridpoint_station_map m ON g.grid_id = m.grid_id AND m.is_primary = true "
                + "JOIN noaa_daily_summary d ON d.station_id = m.station_id "
                + "  AND d.date > (?::date - (? * INTERVAL '1 day')) AND d.date <= ?::date";

        String placeholderSql = "INSERT INTO cached_grid_agg (grid_id, as_of, tmean_c, prcp_30d_mm, last_updated) "
                + "SELECT g.grid_id, ?::date AS as_of, NULL, NULL, now() "
                + "FROM geo_gridpoint g "
                + "WHERE NOT EXISTS (SELECT 1 FROM cached_grid_agg c WHERE c.grid_id = g.grid_id)";

        try (Connection c = ds.getConnection()) {
            // check how many grid rows have data for the window
            try (PreparedStatement pcs = c.prepareStatement(countSql)) {
                pcs.setObject(1, asOf);
                pcs.setInt(2, prcpWindowDays);
                pcs.setObject(3, asOf);
                var rs = pcs.executeQuery();
                int candidateCount = 0;
                if (rs.next())
                    candidateCount = rs.getInt(1);
                log.debug("cached_grid_agg candidates for as_of={} window={}d -> {} grids", asOf, prcpWindowDays,
                        candidateCount);

                if (candidateCount > 0) {
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        ps.setObject(1, asOf);
                        ps.setObject(2, asOf);
                        ps.setInt(3, prcpWindowDays);
                        ps.setObject(4, asOf);
                        int updated = ps.executeUpdate();
                        log.info("cached_grid_agg upsert affected {} rows for as_of={} window={}d", updated, asOf,
                                prcpWindowDays);
                    }
                } else {
                    // no data rows — insert placeholder entries for gridpoints not yet present
                    try (PreparedStatement ph = c.prepareStatement(placeholderSql)) {
                        ph.setObject(1, asOf);
                        int inserted = ph.executeUpdate();
                        log.info("cached_grid_agg inserted {} placeholder rows for as_of={} (no historical data found)",
                                inserted, asOf);
                    }
                }
            }
        } catch (SQLException ex) {
            // If table is missing, create it and retry once
            String state = ex.getSQLState();
            if ("42P01".equals(state) || ex.getMessage().toLowerCase().contains("does not exist")) {
                log.warn("cached_grid_agg table missing, attempting to create it: {}", ex.getMessage());
                createCachedGridAggTable();
                // retry
                try (Connection c2 = ds.getConnection(); PreparedStatement ps = c2.prepareStatement(sql)) {
                    ps.setObject(1, asOf);
                    ps.setObject(2, asOf);
                    ps.setInt(3, prcpWindowDays);
                    ps.setObject(4, asOf);
                    int updated = ps.executeUpdate();
                    log.info("cached_grid_agg upsert retry affected {} rows for as_of={} window={}d", updated, asOf,
                            prcpWindowDays);
                }
            } else {
                throw ex;
            }
        }

        log.info("cached_grid_agg upsert completed for as_of={} window={}d", asOf, prcpWindowDays);
    }

    private void createCachedGridAggTable() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS cached_grid_agg (" +
                "grid_id TEXT PRIMARY KEY REFERENCES geo_gridpoint(grid_id) ON DELETE CASCADE, " +
                "as_of DATE NOT NULL, " +
                "tmean_c REAL, " +
                "prcp_30d_mm REAL, " +
                "last_updated TIMESTAMPTZ DEFAULT now()" +
                ")";

        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(ddl)) {
            ps.executeUpdate();
        }
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "CREATE INDEX IF NOT EXISTS idx_cached_grid_agg_asof ON cached_grid_agg (as_of)")) {
            ps.executeUpdate();
        }
        log.info("created cached_grid_agg table and index");
    }
}
