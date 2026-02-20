package org.sitenetsoft.quarkus.tus.runtime.model;

import java.time.Instant;
import java.util.List;

public class UploadInfo {

    private long entityLength;
    private long offset;
    private String metadata;
    private String creationUrl;
    private boolean isPartial;
    private String uploadConcatMergedValue;
    private Instant expiresAt;
    private boolean deferredLength;
    private boolean isFinalConcat;
    private List<String> partialIds;
    private String uploaderId;

    public long getEntityLength() { return entityLength; }
    public void setEntityLength(long entityLength) { this.entityLength = entityLength; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public String getMetadata() { return metadata; }
    public void setMetadata(String metadata) { this.metadata = metadata; }

    public String getCreationUrl() { return creationUrl; }
    public void setCreationUrl(String creationUrl) { this.creationUrl = creationUrl; }

    public boolean isPartial() { return isPartial; }
    public void setPartial(boolean partial) { isPartial = partial; }

    public String getUploadConcatMergedValue() { return uploadConcatMergedValue; }
    public void setUploadConcatMergedValue(String uploadConcatMergedValue) {
        this.uploadConcatMergedValue = uploadConcatMergedValue;
    }

    public Instant getExpiresAt() { return expiresAt; }
    public void setExpiresAt(Instant expiresAt) { this.expiresAt = expiresAt; }

    public boolean isDeferredLength() { return deferredLength; }
    public void setDeferredLength(boolean deferredLength) { this.deferredLength = deferredLength; }

    public boolean isFinalConcat() { return isFinalConcat; }
    public void setFinalConcat(boolean finalConcat) { isFinalConcat = finalConcat; }

    public List<String> getPartialIds() { return partialIds; }
    public void setPartialIds(List<String> partialIds) { this.partialIds = partialIds; }

    public String getUploaderId() { return uploaderId; }
    public void setUploaderId(String uploaderId) { this.uploaderId = uploaderId; }

    /**
     * Checks if all partial uploads for this final concat are complete.
     */
    public boolean areAllPartialsComplete(java.util.function.Function<String, UploadInfo> lookupFn) {
        if (!isFinalConcat || partialIds == null) {
            return true;
        }
        for (String partialId : partialIds) {
            UploadInfo partial = lookupFn.apply(partialId);
            if (partial == null || partial.getOffset() != partial.getEntityLength()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "UploadInfo{" +
                "entityLength=" + entityLength +
                ", offset=" + offset +
                ", metadata='" + metadata + '\'' +
                ", creationUrl='" + creationUrl + '\'' +
                ", isPartial=" + isPartial +
                '}';
    }

    public static class ChecksumInfo {
        private final String algorithm;
        private final String value;

        public ChecksumInfo(String algorithm, String value) {
            this.algorithm = algorithm;
            this.value = value;
        }

        public String getAlgorithm() { return algorithm; }
        public String getValue() { return value; }
    }
}
