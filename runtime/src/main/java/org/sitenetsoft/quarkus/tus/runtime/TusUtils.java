package org.sitenetsoft.quarkus.tus.runtime;

import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.util.Arrays;
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
}
