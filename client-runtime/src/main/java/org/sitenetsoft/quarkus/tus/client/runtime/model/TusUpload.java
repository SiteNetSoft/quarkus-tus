package org.sitenetsoft.quarkus.tus.client.runtime.model;

import java.time.Instant;
import java.util.Optional;

/**
 * A TUS upload as known to the client: its resource URL, the offset the server has confirmed,
 * its total length, and when it expires (if the server said).
 *
 * @param length the upload's total length in bytes, or {@code -1} if it is deferred/unknown
 */
public record TusUpload(String url, long offset, long length, Optional<Instant> expiresAt) {
}
