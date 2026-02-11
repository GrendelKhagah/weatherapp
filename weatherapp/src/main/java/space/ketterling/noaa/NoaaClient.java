/*
* Copyright 2025 Taylor Ketterling
* NOAA Client for WeatherApp, a weather data ingestion and serving application.
* Utilizes Java HttpClient for making requests to NOAA Climate Data Online (CDO) API V2
* and Jackson for JSON processing.
*/

package space.ketterling.noaa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.ketterling.config.AppConfig;
import space.ketterling.metrics.ExternalApiMetrics;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * HTTP client for NOAA Climate Data Online (CDO) API.
 *
 * <p>
 * Includes rate limiting and a simple circuit breaker to protect NOAA and
 * our service from repeated failures.
 * </p>
 */
public final class NoaaClient {
    // NOAA Climate Data Online (CDO) API v2
    private static final String BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2";

    private final HttpClient http;
    private final AppConfig cfg;
    private final ObjectMapper om;
    // token-bucket rate limiter state (per-process)
    private static final Object TOKEN_LOCK = new Object();
    private static volatile double tokensAvailable = 0.0;
    private static volatile long lastRefillTs = System.currentTimeMillis();

    /**
     * Creates a NOAA client using app config and a shared {@link ObjectMapper}.
     */
    public NoaaClient(AppConfig cfg, ObjectMapper om) {
        this.cfg = cfg;
        this.om = om;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    private static final Logger log = LoggerFactory.getLogger(NoaaClient.class);

    /**
     * Executes a GET request and parses the response as JSON with retries.
     */
    private JsonNode getJson(String url) throws Exception {
        log.debug("NOAA API request -> {}", url);
        int maxAttempts = 3;
        long backoffMs = 1000L;
        // token-bucket limiter: allow up to NOAA_QPS tokens per second (default 1)
        final double qps = Double.parseDouble(System.getenv().getOrDefault("NOAA_QPS", "1"));

        // Simple circuit-breaker (process-wide): protects NOAA when upstream is
        // failing.
        // Configurable via env: NOAA_CB_THRESHOLD, NOAA_CB_WINDOW_MS,
        // NOAA_CB_COOL_DOWN_MS
        final int cbThreshold = Integer.parseInt(System.getenv().getOrDefault("NOAA_CB_THRESHOLD", "5"));
        final long cbWindowMs = Long.parseLong(System.getenv().getOrDefault("NOAA_CB_WINDOW_MS", "60000"));
        final long cbCoolDownMs = Long.parseLong(System.getenv().getOrDefault("NOAA_CB_COOL_DOWN_MS", "300000"));
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            // quick circuit-breaker check
            if (isCircuitOpen(cbCoolDownMs)) {
                String msg = "NOAA circuit-breaker open, skipping requests to NOAA";
                log.warn(msg + " url={}", url);
                throw new RuntimeException(msg);
            }

            // acquire one token (blocking with timeout based on token refill)
            log.debug("NOAA request acquire token attempt={} url={}", attempt, url);
            while (true) {
                long now = System.currentTimeMillis();
                synchronized (TOKEN_LOCK) {
                    double toAdd = ((now - lastRefillTs) / 1000.0) * qps;
                    if (toAdd > 0) {
                        // cap bucket to 10 seconds worth of tokens to avoid burst overload
                        double maxTokens = Math.max(1, qps * 10);
                        tokensAvailable = Math.min(tokensAvailable + toAdd, maxTokens);
                        lastRefillTs = now;
                    }

                    if (tokensAvailable >= 1.0) {
                        tokensAvailable -= 1.0;
                        break; // acquired token
                    }

                    // compute sleep time until next token available
                    double need = 1.0 - tokensAvailable;
                    long waitMs = (long) Math.ceil((need / Math.max(1, qps)) * 1000.0);
                    try {
                        TOKEN_LOCK.wait(waitMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    }
                }
            }
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(60))
                    .header("token", cfg.noaaToken())
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            try {
                log.debug("NOAA request -> {}", url);
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                int code = resp.statusCode();
                if (code >= 200 && code < 300) {
                    log.debug("NOAA response {} for {}", code, url);
                    ExternalApiMetrics.record("NOAA", true);
                    // success -> reset circuit-breaker state
                    recordSuccess();
                    return om.readTree(resp.body());
                }

                ExternalApiMetrics.record("NOAA", false);

                // For retryable status codes, fall through to retry logic
                // Check for Retry-After header for polite backoff
                if (code == 429 || (code >= 500 && code < 600)) {
                    String ra = resp.headers().firstValue("Retry-After").orElse(null);
                    if (ra != null) {
                        try {
                            long raSeconds = Long.parseLong(ra);
                            long waitMillis = raSeconds * 1000L;
                            log.warn("NOAA server asked to Retry-After={}s ({}ms) for url={}", raSeconds, waitMillis,
                                    url);
                            Thread.sleep(waitMillis);
                        } catch (NumberFormatException nfe) {
                            // maybe date format, ignore and use exponential backoff
                        }
                    }
                    if (attempt == maxAttempts) {
                        recordFailure(cbThreshold, cbWindowMs, cbCoolDownMs);
                        throw new RuntimeException(
                                "NOAA request failed: " + code + " url=" + url + " body=" + resp.body());
                    }
                    log.warn("NOAA transient failure code={} url={}, attempt={}", code, url, attempt);
                    recordFailure(cbThreshold, cbWindowMs, cbCoolDownMs);
                } else {
                    // non-retryable failure
                    throw new RuntimeException("NOAA request failed: " + code + " url=" + url + " body=" + resp.body());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            } catch (Exception e) {
                log.warn("NOAA request exception for url={} attempt={} err={}", url, attempt, e.getMessage());
                ExternalApiMetrics.record("NOAA", false);
                recordFailure(cbThreshold, cbWindowMs, cbCoolDownMs);
                if (attempt == maxAttempts) {
                    throw e;
                }
            }

            // exponential backoff before retry
            Thread.sleep(backoffMs);
            backoffMs *= 2;
        }

        throw new RuntimeException("NOAA request failed after retries: " + url);
    }

    // Circuit-breaker state (simple process-local implementation)
    private static volatile int cbFailureCount = 0;
    private static volatile long cbFirstFailureTs = 0L;
    private static volatile boolean cbOpen = false;
    private static volatile long cbOpenUntil = 0L;

    /**
     * Returns true if the circuit breaker is currently open.
     */
    private static boolean isCircuitOpen(long cbCoolDownMs) {
        long now = System.currentTimeMillis();
        if (cbOpen) {
            if (now > cbOpenUntil) {
                // cooldown expired -> close circuit
                cbOpen = false;
                cbFailureCount = 0;
                cbFirstFailureTs = 0L;
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Records a failed request and opens the circuit if failures exceed the
     * threshold.
     */
    private static synchronized void recordFailure(int threshold, long windowMs, long coolDownMs) {
        long now = System.currentTimeMillis();
        if (cbFirstFailureTs == 0L || (now - cbFirstFailureTs) > windowMs) {
            cbFirstFailureTs = now;
            cbFailureCount = 1;
        } else {
            cbFailureCount += 1;
        }

        if (!cbOpen && cbFailureCount >= threshold) {
            cbOpen = true;
            cbOpenUntil = now + coolDownMs;
            log.warn("NOAA circuit-breaker OPEN due to {} failures within {}ms; open until {}", cbFailureCount,
                    windowMs, cbOpenUntil);
        }
    }

    /**
     * Resets circuit breaker state after a successful request.
     */
    private static synchronized void recordSuccess() {
        cbFailureCount = 0;
        cbFirstFailureTs = 0L;
        if (cbOpen) {
            cbOpen = false;
            cbOpenUntil = 0L;
            log.info("NOAA circuit-breaker CLOSED after successful request");
        }
    }

    /**
     * Station search using a small extent bbox around (lat,lon).
     * filter to dataset GHCND (Daily Summaries).
     */
    /**
     * Finds nearby GHCND stations using a bounding box around a point.
     */
    public JsonNode stationsNear(double lat, double lon, double radiusKm, int limit) throws Exception {
        // Build an extent bbox around the point:
        // ~1 deg lat = 111km; lon scale by cos(lat)
        double dLat = radiusKm / 111.0;
        double dLon = radiusKm / (111.0 * Math.max(0.1, Math.cos(Math.toRadians(lat))));

        double minLat = lat - dLat;
        double maxLat = lat + dLat;
        double minLon = lon - dLon;
        double maxLon = lon + dLon;

        String extent = minLat + "," + minLon + "," + maxLat + "," + maxLon;

        String url = BASE + "/stations"
                + "?datasetid=GHCND"
                + "&extent=" + enc(extent)
                + "&sortfield=datacoverage&sortorder=desc"
                + "&limit=" + limit;

        return getJson(url);
    }

    /**
     * Fetch GHCND daily data for a station.
     * datatypeids: TMAX,TMIN,PRCP (add more later if needed).
     */
    /**
     * Fetches daily GHCND data (TMAX/TMIN/PRCP) for a station and date range.
     */
    public JsonNode dailyGhcnd(String stationId, String startDate, String endDate, int limit, int offset)
            throws Exception {
        String url = BASE + "/data"
                + "?datasetid=GHCND"
                + "&stationid=" + enc(stationId)
                + "&startdate=" + enc(startDate)
                + "&enddate=" + enc(endDate)
                + "&datatypeid=TMAX&datatypeid=TMIN&datatypeid=PRCP"
                + "&units=metric"
                + "&limit=" + limit
                + "&offset=" + offset;

        return getJson(url);
    }

    /**
     * URL-encodes a string for safe query parameters.
     */
    private String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
