package space.ketterling.db;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Database access for NWS gridpoints and related lookups.
 */
public class GridpointRepo {
  private final HikariDataSource ds;
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(GridpointRepo.class);

  /**
   * Creates a repo backed by the provided datasource.
   */
  public GridpointRepo(HikariDataSource ds) {
    this.ds = ds;
  }

  /**
   * Inserts or updates a gridpoint row and its forecast URLs.
   */
  public void upsertGridpoint(String gridId, String office, int gridX, int gridY,
      double lat, double lon,
      String gridDataUrl, String hourlyUrl) throws Exception {
    String sql = "INSERT INTO geo_gridpoint (grid_id, office, grid_x, grid_y, geom, forecast_grid_data_url, forecast_hourly_url, last_refreshed_at) "
        +
        "VALUES (?, ?, ?, ?, ST_SetSRID(ST_MakePoint(?, ?), 4326), ?, ?, now()) " +
        "ON CONFLICT (grid_id) DO UPDATE SET " +
        "office=EXCLUDED.office, grid_x=EXCLUDED.grid_x, grid_y=EXCLUDED.grid_y, geom=EXCLUDED.geom, " +
        "forecast_grid_data_url=EXCLUDED.forecast_grid_data_url, forecast_hourly_url=EXCLUDED.forecast_hourly_url, " +
        "last_refreshed_at=now()";

    try (Connection c = ds.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, gridId);
      ps.setString(2, office);
      ps.setInt(3, gridX);
      ps.setInt(4, gridY);
      ps.setDouble(5, lon); // MakePoint(lon,lat)
      ps.setDouble(6, lat);
      ps.setString(7, gridDataUrl);
      ps.setString(8, hourlyUrl);
      ps.executeUpdate();
    }
    log.debug("upsertGridpoint: {} office={} urls=[{},{}]", gridId, office, gridDataUrl, hourlyUrl);
  }

  /**
   * Lists gridpoints that have hourly forecast URLs.
   */
  public List<GridpointRow> listGridpointsForHourly() throws Exception {
    String sql = "SELECT grid_id, forecast_hourly_url FROM geo_gridpoint WHERE forecast_hourly_url IS NOT NULL ORDER BY grid_id";
    List<GridpointRow> out = new ArrayList<>();
    try (Connection c = ds.getConnection();
        PreparedStatement ps = c.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        out.add(new GridpointRow(rs.getString(1), rs.getString(2)));
      }
    }
    return out;
  }

  /**
   * Minimal gridpoint row used for hourly forecast ingest.
   */
  public record GridpointRow(String gridId, String forecastHourlyUrl) {

  }

  /**
   * Lists gridpoints with stored latitude/longitude values.
   */
  public List<GridpointLatLonRow> listGridpointsWithLatLon() throws Exception {
    String sql = "SELECT grid_id, ST_Y(geom) AS lat, ST_X(geom) AS lon FROM geo_gridpoint WHERE geom IS NOT NULL";
    List<GridpointLatLonRow> out = new ArrayList<>();
    try (Connection c = ds.getConnection();
        PreparedStatement ps = c.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        out.add(new GridpointLatLonRow(rs.getString(1), rs.getDouble(2), rs.getDouble(3)));
      }
    }
    return out;
  }

  /**
   * Returns station IDs marked as primary in the gridpoint mapping table.
   */
  public List<String> listPrimaryStations() throws Exception {
    String sql = "SELECT DISTINCT station_id FROM gridpoint_station_map WHERE is_primary=true";
    List<String> out = new ArrayList<>();
    try (Connection c = ds.getConnection();
        PreparedStatement ps = c.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next())
        out.add(rs.getString(1));
    }
    return out;
  }

  /**
   * Finds gridpoints that map to a given station ID.
   */
  public List<String> getGridpointIdsForStation(String stationId) throws Exception {
    String sql = "SELECT grid_id FROM gridpoint_station_map WHERE station_id = ?";
    List<String> out = new ArrayList<>();
    try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, stationId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next())
          out.add(rs.getString(1));
      }
    }
    return out;
  }

  /**
   * Lightweight row containing grid ID and coordinates.
   */
  public record GridpointLatLonRow(String gridId, double lat, double lon) {
  }

  /**
   * Loads full gridpoint details for a single grid ID.
   */
  public GridpointDetail getGridpointById(String gridId) throws Exception {
    String sql = "SELECT grid_id, office, grid_x, grid_y, forecast_grid_data_url, forecast_hourly_url, " +
        "ST_Y(geom) AS lat, ST_X(geom) AS lon " +
        "FROM geo_gridpoint WHERE grid_id = ?";
    try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, gridId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new GridpointDetail(
              rs.getString("grid_id"),
              rs.getString("office"),
              rs.getInt("grid_x"),
              rs.getInt("grid_y"),
              rs.getString("forecast_grid_data_url"),
              rs.getString("forecast_hourly_url"),
              rs.getObject("lat") == null ? null : rs.getDouble("lat"),
              rs.getObject("lon") == null ? null : rs.getDouble("lon"));
        }
      }
    }
    return null;
  }

  /**
   * Full gridpoint details used by APIs and UI.
   */
  public record GridpointDetail(
      String gridId,
      String office,
      int gridX,
      int gridY,
      String forecastGridDataUrl,
      String forecastHourlyUrl,
      Double lat,
      Double lon) {
  }

}
