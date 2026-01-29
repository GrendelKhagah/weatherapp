/*
* Copyright 2025 Taylor Ketterling
* NOAA Station Repository for WeatherApp, a weather data ingestion and serving application.
* Utilizes HikariCP for database connection pooling and performs upsert operations
*/

package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GridpointStationMapRepo {
    private final HikariDataSource ds;

    public GridpointStationMapRepo(HikariDataSource ds) {
        this.ds = ds;
    }

    public void upsertMap(String gridId, String stationId, Double distanceM, boolean isPrimary) throws Exception {
        String sql = "INSERT INTO gridpoint_station_map (grid_id, station_id, distance_m, is_primary) " +
                "VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (grid_id, station_id) DO UPDATE SET " +
                "distance_m=EXCLUDED.distance_m, is_primary=EXCLUDED.is_primary";

        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, gridId);
            ps.setString(2, stationId);
            if (distanceM == null)
                ps.setNull(3, java.sql.Types.REAL);
            else
                ps.setDouble(3, distanceM);
            ps.setBoolean(4, isPrimary);
            ps.executeUpdate();
        }
    }

    public void clearPrimary(String gridId) throws Exception {
        String sql = "UPDATE gridpoint_station_map SET is_primary=false WHERE grid_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, gridId);
            ps.executeUpdate();
        }
    }

    public String getAlternatePrimary(String gridId, String excludeStationId) throws Exception {
        String sql = "SELECT station_id FROM gridpoint_station_map WHERE grid_id = ? AND station_id <> ? ORDER BY distance_m NULLS LAST LIMIT 1";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, gridId);
            ps.setString(2, excludeStationId == null ? "" : excludeStationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getString(1);
            }
        }
        return null;
    }
}
