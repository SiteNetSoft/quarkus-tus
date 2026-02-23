package org.sitenetsoft.quarkus.tus.runtime;

import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public final class TusUtils {

    private TusUtils() {}

    public static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            Pattern.CASE_INSENSITIVE
    );

    public static boolean isValidUuid(String id) {
        if (id == null || id.isBlank()) {
            return false;
        }
        return UUID_PATTERN.matcher(id).matches();
    }

    public static Optional<UploadInfo.ChecksumInfo> parseChecksumHeader(String headerValue) {
        if (headerValue == null) return Optional.empty();
        String[] pair = headerValue.split(" ");
        if (pair.length == 2) {
            return Optional.of(new UploadInfo.ChecksumInfo(pair[0], pair[1]));
        }
        return Optional.empty();
    }

    public static String[] extractPartialUploadIds(String[] fullParts) {
        return Arrays.stream(fullParts)
                .map(TusUtils::getLastBitFromUrl)
                .filter(TusUtils::isValidUuid)
                .toArray(String[]::new);
    }

    private static String getLastBitFromUrl(final String url) {
        return url.replaceFirst(".*/([^/?]+).*", "$1");
    }

    private static final int MAX_METADATA_HEADER_BYTES = 8192;
    private static final int MAX_METADATA_FIELDS = 20;
    private static final int MAX_METADATA_KEY_LENGTH = 64;
    private static final int MAX_METADATA_VALUE_DECODED_LENGTH = 4096;
    private static final Pattern METADATA_KEY_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");

    /**
     * Parses and validates the Upload-Metadata header per TUS protocol spec.
     * Returns a map of key-value pairs if valid, or empty if invalid.
     *
     * @param header the raw Upload-Metadata header value
     * @return parsed metadata map, or empty if validation fails
     */
    public static Optional<Map<String, String>> parseMetadata(String header) {
        if (header == null || header.isBlank()) {
            return Optional.of(Map.of());
        }

        if (header.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_METADATA_HEADER_BYTES) {
            return Optional.empty();
        }

        String[] fields = header.split(",");
        if (fields.length > MAX_METADATA_FIELDS) {
            return Optional.empty();
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (String field : fields) {
            String trimmed = field.trim();
            if (trimmed.isEmpty()) {
                return Optional.empty();
            }

            String[] parts = trimmed.split(" ", 2);
            String key = parts[0].trim();

            if (!isValidMetadataKey(key)) {
                return Optional.empty();
            }

            if (result.containsKey(key)) {
                return Optional.empty();
            }

            String value = "";
            if (parts.length == 2) {
                String encodedValue = parts[1].trim();
                if (!encodedValue.isEmpty()) {
                    try {
                        byte[] decoded = Base64.getDecoder().decode(encodedValue);
                        if (decoded.length > MAX_METADATA_VALUE_DECODED_LENGTH) {
                            return Optional.empty();
                        }
                        value = new String(decoded, java.nio.charset.StandardCharsets.UTF_8);
                    } catch (IllegalArgumentException e) {
                        return Optional.empty();
                    }
                }
            }

            result.put(key, value);
        }

        return Optional.of(result);
    }

    /**
     * Validates a metadata key: alphanumeric, underscore, or hyphen; max 64 chars.
     */
    public static boolean isValidMetadataKey(String key) {
        if (key == null || key.isEmpty() || key.length() > MAX_METADATA_KEY_LENGTH) {
            return false;
        }
        return METADATA_KEY_PATTERN.matcher(key).matches();
    }
}
