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
     * {@code checksum-trailer} is absent: reading HTTP request trailers needs
     * eclipse-vertx/vert.x#5253, which Vert.x does not yet ship. Announcing it would invite
     * clients to rely on verification that never happens.
     */
    @WithDefault("creation,termination,checksum,expiration,concatenation,concatenation-unfinished,creation-with-upload,creation-defer-length")
    String extensions();

    /**
     * Hours before an incomplete upload expires.
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
