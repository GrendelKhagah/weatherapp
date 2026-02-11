package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Database access for user-tracked points (lat/lon + name).
 */
public class TrackedPointRepo {
    private final HikariDataSource ds;

    /**
     * Creates a repo and ensures the table exists.
     */
    public TrackedPointRepo(HikariDataSource ds) {
        this.ds = ds;
        ensureTable();
    }

    /**
     * Creates the tracked_point table if it does not exist.
     */
    private void ensureTable() {
        String sql = """
                CREATE TABLE IF NOT EXISTS tracked_point (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT,
                    lat DOUBLE PRECISION NOT NULL,
                    lon DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(lat, lon)
                )
                """;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.execute();
        } catch (Exception ignored) {
        }
    }

    /**
     * Lists all tracked points in the database.
     */
    public List<TrackedPoint> list() throws Exception {
        String sql = "SELECT id, name, lat, lon, created_at FROM tracked_point ORDER BY id";
        List<TrackedPoint> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new TrackedPoint(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getDouble("lat"),
                        rs.getDouble("lon"),
                        String.valueOf(rs.getObject("created_at"))));
            }
        }
        return out;
    }

    /**
     * Inserts or updates a tracked point and returns its ID.
     */
    public long upsert(String name, double lat, double lon) throws Exception {
        String sql = """
                INSERT INTO tracked_point (name, lat, lon)
                VALUES (?, ?, ?)
                ON CONFLICT (lat, lon) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setDouble(2, lat);
            ps.setDouble(3, lon);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getLong(1);
            }
        }
        return -1;
    }

    /**
     * Deletes a tracked point by ID.
     */
    public int deleteById(long id) throws Exception {
        String sql = "DELETE FROM tracked_point WHERE id = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, id);
            return ps.executeUpdate();
        }
    }

    /**
     * Deletes a tracked point by coordinates.
     */
    public int deleteByLatLon(double lat, double lon) throws Exception {
        String sql = "DELETE FROM tracked_point WHERE lat = ? AND lon = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setDouble(1, lat);
            ps.setDouble(2, lon);
            return ps.executeUpdate();
        }
    }

    /**
     * Small record used for returning tracked point rows.
     */
    public record TrackedPoint(long id, String name, double lat, double lon, String createdAt) {
    }
}