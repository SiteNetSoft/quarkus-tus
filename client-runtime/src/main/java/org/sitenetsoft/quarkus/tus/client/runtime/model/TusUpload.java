package org.sitenetsoft.quarkus.tus.client.runtime.model;

import java.time.Instant;
import java.util.Optional;

public record TusUpload(String url, long offset, long length, Optional<Instant> expiresAt) {
}
