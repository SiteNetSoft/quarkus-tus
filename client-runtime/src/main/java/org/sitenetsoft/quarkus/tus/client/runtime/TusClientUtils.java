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
     *
     * @param metadata map of metadata key-value pairs
     * @return encoded metadata string, empty string for empty map
     */
    public static String encodeMetadata(Map<String, String> metadata) {
        if (metadata.isEmpty()) {
            return "";
        }

        return metadata.entrySet().stream()
            .map(entry -> {
                String key = entry.getKey();
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
