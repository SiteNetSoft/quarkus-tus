package org.sitenetsoft.quarkus.tus.runtime;

import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Builds the {@link UploadInfo} records the framework hands to the store. Everything a record
 * says about the protocol — length, expiry, ownership, concatenation — is decided here, so a
 * store never has to. An {@code expirationHours} of zero means the upload never expires: no
 * deadline is recorded, so nothing enforces one.
 */
final class UploadRecords {

    private UploadRecords() {
    }

    static UploadInfo newUpload(long entityLength, String metadata, boolean partial,
                                boolean deferredLength, String uploaderId, long expirationHours) {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(deferredLength ? -1 : entityLength);
        info.setOffset(0);
        info.setPartial(partial);
        info.setDeferredLength(deferredLength);
        info.setMetadata(metadata);
        info.setUploaderId(uploaderId);
        Instant now = Instant.now();
        info.setLastActivity(now);
        info.setExpiresAt(expirationHours > 0 ? now.plus(expirationHours, ChronoUnit.HOURS) : null);
        return info;
    }

    /**
     * A final concatenation, initially pending: offset 0 and {@code isFinalConcat} until the
     * store's {@code concatenate} fills it. {@code uploadConcatHeader} is stored verbatim
     * because the protocol requires HEAD to echo {@code Upload-Concat} as the client sent it.
     */
    static UploadInfo newFinalConcat(long totalLength, String metadata, String uploaderId,
                                     List<String> partialIds, String uploadConcatHeader,
                                     long expirationHours) {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(totalLength);
        info.setOffset(0);
        info.setPartial(false);
        info.setFinalConcat(true);
        info.setPartialIds(List.copyOf(partialIds));
        info.setUploadConcatMergedValue(uploadConcatHeader);
        info.setMetadata(metadata);
        info.setUploaderId(uploaderId);
        Instant now = Instant.now();
        info.setLastActivity(now);
        info.setExpiresAt(expirationHours > 0 ? now.plus(expirationHours, ChronoUnit.HOURS) : null);
        return info;
    }
}
