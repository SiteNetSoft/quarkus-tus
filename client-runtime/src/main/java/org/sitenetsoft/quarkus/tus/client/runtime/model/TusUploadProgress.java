package org.sitenetsoft.quarkus.tus.client.runtime.model;

/**
 * A progress notification fired after each chunk the client successfully commits to the server.
 *
 * @param bytesSent the number of bytes confirmed by the server so far
 * @param totalBytes the total upload length, or {@code -1} if not known up front
 */
public record TusUploadProgress(long bytesSent, long totalBytes) {
}
