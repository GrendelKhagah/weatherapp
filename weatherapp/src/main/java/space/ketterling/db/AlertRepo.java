package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;

public class AlertRepo {
    private final HikariDataSource ds;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlertRepo.class);

    public AlertRepo(HikariDataSource ds) {
        this.ds = ds;
    }

    public void upsertAlert(String alertId,
            String event,
            String severity,
            String certainty,
            String urgency,
            String headline,
            String description,
            String instruction,
            Instant effective,
            Instant onset,
            Instant expires,
            Instant ends,
            String status,
            String messageType,
            String areaDesc,
            String geometryGeoJson, // may be null
            String rawJson) throws Exception {

        String sql = "INSERT INTO nws_alert (" +
                "alert_id, event, severity, certainty, urgency, headline, description, instruction, " +
                "effective, onset, expires, ends, status, message_type, area_desc, geom, raw_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "CASE WHEN ? IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromGeoJSON(?), 4326) END, ?::jsonb) " +
                "ON CONFLICT (alert_id) DO UPDATE SET " +
                "event=EXCLUDED.event, severity=EXCLUDED.severity, certainty=EXCLUDED.certainty, urgency=EXCLUDED.urgency, "
                +
                "headline=EXCLUDED.headline, description=EXCLUDED.description, instruction=EXCLUDED.instruction, " +
                "effective=EXCLUDED.effective, onset=EXCLUDED.onset, expires=EXCLUDED.expires, ends=EXCLUDED.ends, " +
                "status=EXCLUDED.status, message_type=EXCLUDED.message_type, area_desc=EXCLUDED.area_desc, " +
                "geom=EXCLUDED.geom, raw_json=EXCLUDED.raw_json, ingested_at=now()";

        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, alertId);
            ps.setString(2, event);
            ps.setString(3, severity);
            ps.setString(4, certainty);
            ps.setString(5, urgency);
            ps.setString(6, headline);
            ps.setString(7, description);
            ps.setString(8, instruction);

            setInstant(ps, 9, effective);
            setInstant(ps, 10, onset);
            setInstant(ps, 11, expires);
            setInstant(ps, 12, ends);

            ps.setString(13, status);
            ps.setString(14, messageType);
            ps.setString(15, areaDesc);

            // geometry parameter is used twice in the CASE expression
            if (geometryGeoJson == null) {
                ps.setNull(16, java.sql.Types.VARCHAR);
                ps.setNull(17, java.sql.Types.VARCHAR);
            } else {
                ps.setString(16, geometryGeoJson);
                ps.setString(17, geometryGeoJson);
            }

            ps.setString(18, rawJson);
            ps.executeUpdate();
        }
        log.debug("upsertAlert: alertId={} event={} severity={}", alertId, event, severity);
    }

    private void setInstant(PreparedStatement ps, int idx, Instant t) throws Exception {
        if (t == null)
            ps.setNull(idx, java.sql.Types.TIMESTAMP);
        else
            ps.setTimestamp(idx, Timestamp.from(t));
    }
}
