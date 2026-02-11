package space.ketterling.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks success/failure counts for external API calls (NWS, NOAA, etc.).
 *
 * <p>
 * Uses a rolling 60-minute window to compute basic health status.
 * </p>
 */
public final class ExternalApiMetrics {
    private static final int WINDOW_MINUTES = 60;
    private static final Map<String, ServiceBuckets> SERVICES = new ConcurrentHashMap<>();

    /**
     * Utility class; no instances.
     */
    private ExternalApiMetrics() {
    }

    /**
     * Records one call outcome for a named external service.
     */
    public static void record(String service, boolean success) {
        if (service == null || service.isBlank())
            return;
        ServiceBuckets buckets = SERVICES.computeIfAbsent(service, k -> new ServiceBuckets());
        buckets.record(success);
    }

    /**
     * Returns a snapshot of call counts and failure rates by service.
     */
    public static Map<String, ServiceSnapshot> snapshot() {
        Map<String, ServiceSnapshot> out = new ConcurrentHashMap<>();
        for (var e : SERVICES.entrySet()) {
            out.put(e.getKey(), e.getValue().snapshot());
        }
        return out;
    }

    /**
     * Returns the rolling window size (minutes) used for metrics.
     */
    public static int windowMinutes() {
        return WINDOW_MINUTES;
    }

    /**
     * Summary metrics for a single external service.
     */
    public static final class ServiceSnapshot {
        public final long callsLastHour;
        public final long failuresLastHour;
        public final double failurePct;
        public final String status;

        private ServiceSnapshot(long callsLastHour, long failuresLastHour, double failurePct, String status) {
            this.callsLastHour = callsLastHour;
            this.failuresLastHour = failuresLastHour;
            this.failurePct = failurePct;
            this.status = status;
        }
    }

    /**
     * Ring buffer of per-minute counts for a service.
     */
    private static final class ServiceBuckets {
        private final long[] total = new long[WINDOW_MINUTES];
        private final long[] fail = new long[WINDOW_MINUTES];
        private final long[] minute = new long[WINDOW_MINUTES];

        /**
         * Records success/failure in the current minute bucket.
         */
        private synchronized void record(boolean success) {
            long nowMin = System.currentTimeMillis() / 60000L;
            int idx = (int) (nowMin % WINDOW_MINUTES);
            if (minute[idx] != nowMin) {
                minute[idx] = nowMin;
                total[idx] = 0L;
                fail[idx] = 0L;
            }
            total[idx] += 1L;
            if (!success) {
                fail[idx] += 1L;
            }
        }

        /**
         * Builds a snapshot by summing buckets from the last hour.
         */
        private synchronized ServiceSnapshot snapshot() {
            long nowMin = System.currentTimeMillis() / 60000L;
            long totalSum = 0L;
            long failSum = 0L;
            for (int i = 0; i < WINDOW_MINUTES; i++) {
                long bucketMin = minute[i];
                if (bucketMin == 0L)
                    continue;
                if ((nowMin - bucketMin) >= WINDOW_MINUTES)
                    continue;
                totalSum += total[i];
                failSum += fail[i];
            }
            double failurePct = totalSum == 0 ? 0.0 : (failSum * 100.0) / totalSum;
            String status;
            if (totalSum == 0) {
                status = "no-data";
            } else if (failurePct >= 50.0) {
                status = "down";
            } else if (failurePct >= 10.0) {
                status = "degraded";
            } else {
                status = "ok";
            }
            return new ServiceSnapshot(totalSum, failSum, failurePct, status);
        }
    }
}
