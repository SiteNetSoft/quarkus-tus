package org.sitenetsoft.quarkus.tus.client.runtime;

import org.sitenetsoft.quarkus.tus.client.runtime.error.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;

public final class TusClientUtils {

    private TusClientUtils() {
        // utility class
    }

    /**
     * Encodes metadata as per TUS spec: comma-separated key base64value pairs.
     * Keys are never encoded, empty values are represented as key alone.
     * Keys must not contain spaces, commas, or newlines (they are delimiters).
     *
     * @param metadata map of metadata key-value pairs
     * @return encoded metadata string, empty string for empty map
     * @throws IllegalArgumentException if any key is null, empty, or contains forbidden characters
     */
    public static String encodeMetadata(Map<String, String> metadata) {
        if (metadata.isEmpty()) {
            return "";
        }

        return metadata.entrySet().stream()
            .map(entry -> {
                String key = entry.getKey();
                validateKey(key);
                String value = entry.getValue();
                if (value == null || value.isEmpty()) {
                    return key;
                }
                String encoded = Base64.getEncoder()
                    .encodeToString(value.getBytes(StandardCharsets.UTF_8));
                return key + " " + encoded;
            })
            .collect(Collectors.joining(","));
    }

    /**
     * Validates that a metadata key is not null, empty, or contains forbidden characters.
     *
     * @param key the key to validate
     * @throws IllegalArgumentException if key is invalid
     */
    private static void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Metadata key cannot be null");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Metadata key cannot be empty");
        }
        if (key.contains(" ")) {
            throw new IllegalArgumentException("Metadata key cannot contain spaces: " + key);
        }
        if (key.contains(",")) {
            throw new IllegalArgumentException("Metadata key cannot contain commas: " + key);
        }
        if (key.contains("\n") || key.contains("\r")) {
            throw new IllegalArgumentException("Metadata key cannot contain newlines: " + key);
        }
    }

    /**
     * Maps HTTP status codes to typed TusClientException subclasses.
     *
     * @param status HTTP status code
     * @param expired whether the upload is known to have expired (for 410)
     * @return the appropriate TusClientException
     */
    public static TusClientException fromStatus(int status, boolean expired) {
        return switch (status) {
            case 409 -> new TusOffsetMismatchException("Offset mismatch");
            case 404 -> new TusUploadNotFoundException("Upload not found", false);
            case 410 -> new TusUploadNotFoundException("Upload gone", expired);
            case 412 -> new TusVersionMismatchException("Protocol version mismatch");
            case 413 -> new TusPayloadTooLargeException("Payload too large");
            case 423 -> new TusUploadLockedException("Upload is locked by another request");
            case 460 -> new TusChecksumMismatchException("Checksum mismatch");
            default -> {
                if (status >= 500) {
                    yield new TusServerErrorException("Server error: " + status, status);
                }
                yield new TusProtocolException("Unexpected status code: " + status);
            }
        };
    }
}
