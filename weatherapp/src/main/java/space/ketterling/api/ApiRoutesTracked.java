package space.ketterling.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.ketterling.db.TrackedPointRepo;

/**
 * Routes that manage user-tracked points on the map.
 */
final class ApiRoutesTracked {
    private static final Logger log = LoggerFactory.getLogger(ApiRoutesTracked.class);

    /**
     * Utility class; do not instantiate.
     */
    private ApiRoutesTracked() {
    }

    /**
     * Registers tracked point endpoints (list/add/delete/refresh).
     */
    static void register(ApiServer api) {
        Javalin app = api.app();
        ObjectMapper om = api.om();
        TrackedPointRepo trackedPointRepo = api.trackedPointRepo();

        app.get("/api/tracked-points", ctx -> {
            ArrayNode arr = om.createArrayNode();
            try {
                for (var p : trackedPointRepo.list()) {
                    ObjectNode row = om.createObjectNode();
                    row.put("id", p.id());
                    row.put("name", p.name());
                    row.put("lat", p.lat());
                    row.put("lon", p.lon());
                    row.put("created_at", p.createdAt());
                    arr.add(row);
                }
            } catch (Exception e) {
                log.warn("Failed to list tracked points: {}", e.getMessage());
                ctx.status(500).json(om.createObjectNode().put("error", "tracked_points_list_failed"));
                return;
            }
            ctx.json(arr);
        });

        app.post("/api/tracked-points", ctx -> {
            String name = ctx.queryParam("name");
            Double lat = null;
            Double lon = null;
            try {
                lat = Double.parseDouble(ctx.queryParam("lat"));
                lon = Double.parseDouble(ctx.queryParam("lon"));
            } catch (Exception ignored) {
            }
            if (lat == null || lon == null) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat and lon are required"));
                return;
            }
            if (!ApiServer.isLatLonValid(lat, lon)) {
                ctx.status(400).json(om.createObjectNode().put("error", "lat or lon out of range"));
                return;
            }
            if (name == null || name.isBlank()) {
                name = "Tracked Point";
            }

            try {
                long id = trackedPointRepo.upsert(name, lat, lon);
                ctx.json(om.createObjectNode().put("id", id).put("status", "ok"));
            } catch (Exception e) {
                log.warn("Failed to upsert tracked point: {}", e.getMessage());
                ctx.status(500).json(om.createObjectNode().put("error", "tracked_point_upsert_failed"));
            }
        });

        app.delete("/api/tracked-points", ctx -> {
            String idStr = ctx.queryParam("id");
            Integer deleted = null;
            try {
                if (idStr != null && !idStr.isBlank()) {
                    deleted = trackedPointRepo.deleteById(Long.parseLong(idStr));
                } else {
                    Double lat = Double.parseDouble(ctx.queryParam("lat"));
                    Double lon = Double.parseDouble(ctx.queryParam("lon"));
                    deleted = trackedPointRepo.deleteByLatLon(lat, lon);
                }
            } catch (Exception e) {
                ctx.status(400).json(om.createObjectNode().put("error", "invalid id or lat/lon"));
                return;
            }

            ctx.json(om.createObjectNode().put("deleted", deleted == null ? 0 : deleted));
        });

        app.post("/api/tracked-points/refresh", ctx -> {
            Runnable trigger = api.gridpointRefreshTrigger();
            if (trigger == null) {
                ctx.status(503).json(om.createObjectNode().put("error", "refresh_not_available"));
                return;
            }

            new Thread(() -> {
                try {
                    trigger.run();
                } catch (Exception e) {
                    log.warn("Manual gridpoint refresh failed: {}", e.getMessage());
                }
            }, "api-gridpoint-refresh").start();

            ctx.json(om.createObjectNode().put("status", "started"));
        });
    }
}
