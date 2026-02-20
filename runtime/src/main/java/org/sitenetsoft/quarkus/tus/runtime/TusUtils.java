package org.sitenetsoft.quarkus.tus.runtime;

import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.nio.charset.StandardCharsets;
import java.util.*;

public final class TusUtils {

    private static final Logger LOG = Logger.getLogger(TusUtils.class);

    private TusUtils() {}

    public static String sanitizeFilename(String original, String fallbackId) {
        if (original == null || original.isBlank()) {
            return "upload-" + fallbackId;
        }

        String base = original.replace("\\", "/");
        int idx = base.lastIndexOf('/');
        if (idx >= 0 && idx < base.length() - 1) {
            base = base.substring(idx + 1);
        }

        base = java.text.Normalizer.normalize(base, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        base = base.replaceAll("[^a-zA-Z0-9._-]", "_");
        base = base.replaceAll("_+", "_");

        if (base.isBlank()) {
            base = "upload-" + fallbackId;
        }

        if (base.length() > 255) {
            base = base.substring(0, 255);
        }

        return base;
    }

    private static final int MAX_METADATA_FIELDS = 20;
    private static final int MAX_KEY_LENGTH = 64;
    private static final int MAX_VALUE_LENGTH = 4096;
    private static final int MAX_HEADER_LENGTH = 8192;

    public static Map<String, String> parseMetadata(String uploadMetadataHeader) {
        Map<String, String> result = new HashMap<>();
        if (uploadMetadataHeader == null || uploadMetadataHeader.isBlank()) {
            return result;
        }

        if (uploadMetadataHeader.length() > MAX_HEADER_LENGTH) {
            LOG.warnf("Upload-Metadata header too long: %d bytes (max %d)",
                    uploadMetadataHeader.length(), MAX_HEADER_LENGTH);
            return result;
        }

        String[] pairs = uploadMetadataHeader.split(",");

        if (pairs.length > MAX_METADATA_FIELDS) {
            LOG.warnf("Too many metadata fields: %d (max %d)", pairs.length, MAX_METADATA_FIELDS);
            return result;
        }

        for (String pair : pairs) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) continue;

            String[] parts = trimmed.split(" ", 2);
            String key = parts[0];

            if (key.length() > MAX_KEY_LENGTH) {
                LOG.warnf("Metadata key too long: %d chars (max %d)", key.length(), MAX_KEY_LENGTH);
                continue;
            }
            if (!isValidMetadataKey(key)) {
                LOG.warnf("Invalid metadata key rejected: %s", key);
                continue;
            }

            String b64 = parts.length > 1 ? parts[1] : "";
            String value;
            try {
                value = new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
                if (value.length() > MAX_VALUE_LENGTH) {
                    LOG.warnf("Metadata value too long for key '%s': %d chars (max %d) - rejected",
                            key, value.length(), MAX_VALUE_LENGTH);
                    continue;
                }
            } catch (IllegalArgumentException ex) {
                LOG.warnf(ex, "Invalid base64 value in Upload-Metadata for key '%s': '%s'", key, b64);
                value = "";
            }
            result.put(key, value);
        }

        return result;
    }

    private static boolean isValidMetadataKey(String key) {
        if (key == null || key.isEmpty()) return false;
        for (char c : key.toCharArray()) {
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '-') {
                return false;
            }
        }
        return true;
    }

    public static Optional<UploadInfo.ChecksumInfo> parseChecksumHeader(String headerValue) {
        if (headerValue == null) return Optional.empty();
        String[] pair = headerValue.split(" ");
        if (pair.length == 2) {
            return Optional.of(new UploadInfo.ChecksumInfo(pair[0], pair[1]));
        }
        return Optional.empty();
    }

    private static final java.util.regex.Pattern UUID_PATTERN = java.util.regex.Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            java.util.regex.Pattern.CASE_INSENSITIVE
    );

    public static String[] extractPartialUploadIds(String[] fullParts) {
        return Arrays.stream(fullParts)
                .map(TusUtils::getLastBitFromUrl)
                .filter(TusUtils::isValidUuid)
                .toArray(String[]::new);
    }

    private static String getLastBitFromUrl(final String url) {
        return url.replaceFirst(".*/([^/?]+).*", "$1");
    }

    private static boolean isValidUuid(String id) {
        if (id == null || id.isBlank()) {
            return false;
        }
        boolean valid = UUID_PATTERN.matcher(id).matches();
        if (!valid) {
            LOG.warnf("Invalid upload ID format rejected during extraction: %s", id);
        }
        return valid;
    }

    public static String escapeForJson(String input) {
        if (input == null) return null;
        StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < ' ') {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
