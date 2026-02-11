/*
* Copyright 2025 Taylor Ketterling
* NOAA Station Repository for WeatherApp, a weather data ingestion and serving application.
* Utilizes HikariCP for database connection pooling and performs upsert operations
* on NOAA station data into a PostgreSQL/PostGIS database.
*/

package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Database access for NOAA station metadata.
 */
public class NoaaStationRepo {
    private final HikariDataSource ds;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NoaaStationRepo.class);

    /**
     * Creates a repo backed by the provided datasource.
     */
    public NoaaStationRepo(HikariDataSource ds) {
        this.ds = ds;
    }

    /**
     * Inserts or updates a NOAA station row.
     */
    public void upsertStation(String stationId, String name, Double lat, Double lon, Double elevationM,
            String metadataJson) throws Exception {
        String sql = "INSERT INTO noaa_station (station_id, name, geom, elevation_m, metadata) " +
                "VALUES (?, ?, " +
                "CASE WHEN ? IS NULL OR ? IS NULL THEN NULL ELSE ST_SetSRID(ST_MakePoint(?, ?), 4326) END, " +
                "?, ?::jsonb) " +
                "ON CONFLICT (station_id) DO UPDATE SET " +
                "name=EXCLUDED.name, geom=EXCLUDED.geom, elevation_m=EXCLUDED.elevation_m, metadata=EXCLUDED.metadata";

        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, stationId);
            ps.setString(2, name);

            // CASE uses lat/lon twice
            if (lat == null || lon == null) {
                ps.setNull(3, java.sql.Types.DOUBLE);
                ps.setNull(4, java.sql.Types.DOUBLE);
                ps.setNull(5, java.sql.Types.DOUBLE);
                ps.setNull(6, java.sql.Types.DOUBLE);
            } else {
                ps.setDouble(3, lat);
                ps.setDouble(4, lon);
                ps.setDouble(5, lon); // MakePoint(lon,lat)
                ps.setDouble(6, lat);
            }

            if (elevationM == null)
                ps.setNull(7, java.sql.Types.REAL);
            else
                ps.setDouble(7, elevationM);

            ps.setString(8, metadataJson);
            ps.executeUpdate();
        }

        log.debug("upsertStation: {}", stationId);
    }
}
