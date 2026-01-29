package space.ketterling.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ExternalApiMetrics {
    private static final int WINDOW_MINUTES = 60;
    private static final Map<String, ServiceBuckets> SERVICES = new ConcurrentHashMap<>();

    private ExternalApiMetrics() {
    }

    public static void record(String service, boolean success) {
        if (service == null || service.isBlank())
            return;
        ServiceBuckets buckets = SERVICES.computeIfAbsent(service, k -> new ServiceBuckets());
        buckets.record(success);
    }

    public static Map<String, ServiceSnapshot> snapshot() {
        Map<String, ServiceSnapshot> out = new ConcurrentHashMap<>();
        for (var e : SERVICES.entrySet()) {
            out.put(e.getKey(), e.getValue().snapshot());
        }
        return out;
    }

    public static int windowMinutes() {
        return WINDOW_MINUTES;
    }

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

    private static final class ServiceBuckets {
        private final long[] total = new long[WINDOW_MINUTES];
        private final long[] fail = new long[WINDOW_MINUTES];
        private final long[] minute = new long[WINDOW_MINUTES];

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
