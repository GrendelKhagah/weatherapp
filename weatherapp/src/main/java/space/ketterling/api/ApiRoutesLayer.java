package space.ketterling.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;

/**
 * Routes that generate map layers like temperature and precipitation.
 */
final class ApiRoutesLayer {
    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesLayer() {
    }

    /**
     * Registers map layer endpoints used by the UI.
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        HikariDataSource ds = api.ds();

        app.get("/layers/temperature", ctx -> {
            int hourOffset = ApiServer.parseInt(ctx.queryParam("hourOffset"), 0, 0, 24);
            double[] bbox = api.parseBbox(ctx.queryParam("bbox"));
            Instant target = Instant.now().plusSeconds(hourOffset * 3600L);

            ObjectNode fc = api.featureCollection();
            StringBuilder sql = new StringBuilder("""
                    WITH stations AS (
                        SELECT s.station_id,
                               s.geom,
                               (d.tmax_c + d.tmin_c) / 2.0 AS tmean_c
                        FROM noaa_station s
                        JOIN LATERAL (
                            SELECT tmax_c, tmin_c
                            FROM noaa_daily_summary
                            WHERE station_id = s.station_id
                            ORDER BY date DESC
                            LIMIT 1
                        ) d ON true
                        WHERE s.geom IS NOT NULL
                          AND d.tmax_c IS NOT NULL
                          AND d.tmin_c IS NOT NULL
                    ),
                    grid AS (
                        SELECT g.grid_id, g.geom
                        FROM geo_gridpoint g
                        WHERE g.geom IS NOT NULL
                    """);

            if (bbox != null) {
                sql.append(" AND ST_Intersects(g.geom, ST_MakeEnvelope(?, ?, ?, ?, 4326)) ");
            }

            sql.append("""
                    )
                    SELECT g.grid_id,
                           ST_Y(g.geom) AS lat,
                           ST_X(g.geom) AS lon,
                           SUM(s.tmean_c * s.weight) / NULLIF(SUM(s.weight), 0) AS temperature_c
                    FROM grid g
                    JOIN LATERAL (
                        SELECT st.tmean_c,
                               CASE
                                   WHEN st.dist_km < 0.001 THEN 1000000.0
                                   ELSE 1.0 / (st.dist_km * st.dist_km)
                               END AS weight
                        FROM (
                            SELECT st.tmean_c,
                                   ST_Distance(g.geom::geography, st.geom::geography) / 1000.0 AS dist_km
                            FROM stations st
                            ORDER BY g.geom <-> st.geom
                            LIMIT 6
                        ) st
                    ) s ON true
                    GROUP BY g.grid_id, g.geom
                    """);

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
                int i = 1;
                if (bbox != null) {
                    ps.setDouble(i++, bbox[0]);
                    ps.setDouble(i++, bbox[1]);
                    ps.setDouble(i++, bbox[2]);
                    ps.setDouble(i++, bbox[3]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");
                        ObjectNode props = om.createObjectNode();
                        props.put("grid_id", rs.getString("grid_id"));
                        api.putNullable(props, "temperature_c", rs.getObject("temperature_c"));
                        feat.set("properties", props);
                        String geom = String.format("{\"type\":\"Point\",\"coordinates\":[%f,%f]}",
                                rs.getDouble("lon"), rs.getDouble("lat"));
                        feat.set("geometry", om.readTree(geom));
                        feats.add(feat);
                    }
                }
            }

            ctx.json(fc);
        });

        app.get("/layers/precipitation", ctx -> {
            String range = ctx.queryParam("range");
            int days = 30;
            if (range != null && range.endsWith("d")) {
                try {
                    days = Integer.parseInt(range.substring(0, range.length() - 1));
                } catch (Exception ignored) {
                }
            }

            ObjectNode fc = api.featureCollection();
            String sql = "SELECT s.station_id, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon, SUM(d.prcp_mm) AS prcp_mm "
                    + "FROM noaa_station s "
                    + "JOIN noaa_daily_summary d ON d.station_id = s.station_id "
                    + "WHERE s.geom IS NOT NULL "
                    + "AND d.date >= current_date - (? * INTERVAL '1 day') AND d.date < current_date "
                    + "GROUP BY s.station_id, s.geom";

            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, days);
                try (ResultSet rs = ps.executeQuery()) {
                    ArrayNode feats = (ArrayNode) fc.get("features");
                    while (rs.next()) {
                        ObjectNode feat = om.createObjectNode();
                        feat.put("type", "Feature");
                        ObjectNode props = om.createObjectNode();
                        props.put("station_id", rs.getString("station_id"));
                        api.putNullable(props, "prcp_mm", rs.getObject("prcp_mm"));
                        feat.set("properties", props);
                        String geom = String.format("{\"type\":\"Point\",\"coordinates\":[%f,%f]}", rs.getDouble("lon"),
                                rs.getDouble("lat"));
                        feat.set("geometry", om.readTree(geom));
                        feats.add(feat);
                    }
                }
            }

            ctx.json(fc);
        });
    }
}
