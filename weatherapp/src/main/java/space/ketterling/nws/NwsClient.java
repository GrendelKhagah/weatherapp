package space.ketterling.nws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import space.ketterling.config.AppConfig;
import space.ketterling.metrics.ExternalApiMetrics;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

public final class NwsClient {
    private final HttpClient http;
    private final AppConfig cfg;
    private final ObjectMapper om;

    private static final long ALERT_TTL_MS = Long.parseLong(System.getenv().getOrDefault("NWS_ALERT_TTL_SECONDS", "60"))
            * 1000L;
    private static final long FORECAST_TTL_MS = Long
            .parseLong(System.getenv().getOrDefault("NWS_FORECAST_TTL_SECONDS", "21600")) * 1000L;
    private static final long POINTS_TTL_MS = Long
            .parseLong(System.getenv().getOrDefault("NWS_POINTS_TTL_SECONDS", "86400")) * 1000L;

    private static final ConcurrentHashMap<String, CacheEntry> CACHE = new ConcurrentHashMap<>();

    public NwsClient(AppConfig cfg, ObjectMapper om) {
        this.cfg = cfg;
        this.om = om;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public JsonNode getJson(String url) throws Exception {
        long ttlMs = cacheTtlForUrl(url);
        if (ttlMs > 0) {
            CacheEntry cached = CACHE.get(url);
            if (cached != null && cached.expiresAtMs > System.currentTimeMillis()) {
                return cached.body.deepCopy();
            }
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("User-Agent", cfg.nwsUserAgent())
                .header("Accept", "application/geo+json")
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            ExternalApiMetrics.record("NWS", false);
            throw new RuntimeException(
                    "NWS request failed: " + resp.statusCode() + " url=" + url + " body=" + resp.body());
        }
        ExternalApiMetrics.record("NWS", true);
        JsonNode json = om.readTree(resp.body());
        if (ttlMs > 0) {
            CACHE.put(url, new CacheEntry(System.currentTimeMillis() + ttlMs, json));
        }
        return json;
    }

    public JsonNode points(double lat, double lon) throws Exception {
        return getJson("https://api.weather.gov/points/" + lat + "," + lon);
    }

    public JsonNode forecastHourly(String forecastHourlyUrl) throws Exception {
        return getJson(forecastHourlyUrl);
    }

    // Simple + reliable approach: query alerts for each tracked point
    public JsonNode activeAlertsForPoint(double lat, double lon) throws Exception {
        return getJson("https://api.weather.gov/alerts/active?point=" + lat + "," + lon);
    }

    private static long cacheTtlForUrl(String url) {
        if (url == null)
            return 0L;
        String u = url.toLowerCase();
        if (u.contains("/alerts/active"))
            return ALERT_TTL_MS;
        if (u.contains("/forecast/hourly") || u.contains("/forecast"))
            return FORECAST_TTL_MS;
        if (u.contains("/points/"))
            return POINTS_TTL_MS;
        return 0L;
    }

    private static final class CacheEntry {
        final long expiresAtMs;
        final JsonNode body;

        CacheEntry(long expiresAtMs, JsonNode body) {
            this.expiresAtMs = expiresAtMs;
            this.body = body;
        }
    }
}
