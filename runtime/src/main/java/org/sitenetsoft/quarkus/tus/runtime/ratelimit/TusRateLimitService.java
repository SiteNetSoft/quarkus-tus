package org.sitenetsoft.quarkus.tus.runtime.ratelimit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;

import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class TusRateLimitService {

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public RateLimitResult tryConsume(String clientId) {
        TokenBucket bucket = buckets.computeIfAbsent(clientId,
                k -> new TokenBucket(tusRuntimeConfig.rateLimitBurstSize(),
                        tusRuntimeConfig.rateLimitRequestsPerMinute()));
        return bucket.tryConsume();
    }

    public void cleanupIdleBuckets() {
        long cutoff = System.currentTimeMillis() - 3_600_000L; // 1 hour
        buckets.entrySet().removeIf(entry -> entry.getValue().getLastAccessTime() < cutoff);
    }

    public record RateLimitResult(boolean allowed, long retryAfterMs) {}

    static class TokenBucket {
        private final int burstSize;
        private final double refillRatePerMs; // tokens per millisecond
        private double tokens;
        private long lastRefillTime;
        private long lastAccessTime;

        TokenBucket(int burstSize, int requestsPerMinute) {
            this.burstSize = burstSize;
            this.refillRatePerMs = requestsPerMinute / 60_000.0;
            this.tokens = burstSize;
            this.lastRefillTime = System.currentTimeMillis();
            this.lastAccessTime = this.lastRefillTime;
        }

        synchronized RateLimitResult tryConsume() {
            long now = System.currentTimeMillis();
            lastAccessTime = now;

            // Refill tokens based on elapsed time
            long elapsed = now - lastRefillTime;
            if (elapsed > 0) {
                tokens = Math.min(burstSize, tokens + elapsed * refillRatePerMs);
                lastRefillTime = now;
            }

            if (tokens >= 1.0) {
                tokens -= 1.0;
                return new RateLimitResult(true, 0);
            }

            // Calculate how long until one token is available
            double deficit = 1.0 - tokens;
            long retryAfterMs = (long) Math.ceil(deficit / refillRatePerMs);
            return new RateLimitResult(false, retryAfterMs);
        }

        long getLastAccessTime() {
            return lastAccessTime;
        }
    }
}
