package org.sitenetsoft.quarkus.tus.runtime.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.tus")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface TusRuntimeConfig {

    /**
     * TUS protocol version.
     */
    @WithDefault("1.0.0")
    String version();

    /**
     * Maximum upload size in bytes.
     */
    @WithDefault("107374182400")
    long maxSize();

    /**
     * Active TUS protocol extensions (comma-separated).
     * <p>
     * {@code checksum-trailer} requires a vertx-core that can read HTTP request trailers
     * (eclipse-vertx/vert.x#5253). Remove it from this list when running against a stock
     * Vert.x, otherwise clients will rely on verification that never happens.
     */
    @WithDefault("creation,termination,checksum,checksum-trailer,expiration,concatenation,concatenation-unfinished,creation-with-upload,creation-defer-length")
    String extensions();

    /**
     * Hours after creation at which an upload expires and is removed, complete or not — the
     * application is expected to have moved a finished upload on by then. Set to 0 to disable
     * expiry (no deadline is recorded).
     */
    @WithDefault("24")
    long expirationHours();

    /**
     * Supported checksum algorithms (comma-separated).
     */
    @WithDefault("sha1,md5,sha256")
    String checksumAlgorithms();

    /**
     * Maximum requests per minute per client for rate limiting.
     */
    @WithDefault("60")
    int rateLimitRequestsPerMinute();

    /**
     * Maximum burst size for rate limiting (initial token count).
     */
    @WithDefault("10")
    int rateLimitBurstSize();

    /**
     * Local store configuration.
     */
    StoreConfig store();

    /**
     * Maximum chunk size per PATCH request in bytes.
     */
    @WithDefault("10485760")
    long maxChunkSize();

    /**
     * Hours of inactivity before an incomplete upload is considered stale and eligible for cleanup.
     * Set to 0 to disable stale upload cleanup.
     */
    @WithDefault("6")
    long staleUploadHours();

    /**
     * Seconds after which an upload lock that has seen no activity is considered abandoned and
     * may be reclaimed. A lock is refreshed as long as bytes are flowing, so this only bounds a
     * holder that died or stalled — set it above the longest pause a healthy client may make
     * mid-chunk. Set to 0 to disable reclamation: a lock is then held until released. Applies
     * to the bundled local file store.
     */
    @WithDefault("30")
    long lockTimeoutSeconds();

    /**
     * Seconds a held-open SSE events stream may outlive its upload's completion before the
     * framework closes it anyway. The backstop for a consumer that called
     * {@code TusSseService.holdOpen} but whose {@code finish} never comes — without it an
     * abandoned pipeline would leak the sink forever. Counted from the completion event.
     */
    @WithDefault("300")
    long sseHoldOpenTimeoutSeconds();

    /**
     * Maximum number of partial uploads a single {@code Upload-Concat: final} request may
     * reference. Bounds how much copying one request can schedule; without it the only ceiling
     * is the HTTP header size limit, which is not a deliberate bound.
     */
    @WithDefault("1000")
    int maxConcatParts();

    interface StoreConfig {
        /**
         * Local file store configuration.
         */
        LocalConfig local();

        interface LocalConfig {
            /**
             * Directory for upload storage.
             */
            @WithDefault("${java.io.tmpdir}/quarkus-tus-uploads")
            String uploadDir();
        }
    }
}
