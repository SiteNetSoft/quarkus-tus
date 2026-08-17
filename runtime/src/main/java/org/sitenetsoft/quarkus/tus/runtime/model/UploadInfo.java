package org.sitenetsoft.quarkus.tus.runtime.model;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class UploadInfo {

    private long entityLength;
    private long offset;
    private String metadata;
    private boolean isPartial;
    private String uploadConcatMergedValue;
    private Instant expiresAt;
    private boolean deferredLength;
    private boolean isFinalConcat;
    private List<String> partialIds;
    private String uploaderId;
    private Instant lastActivity;

    public long getEntityLength() { return entityLength; }
    public void setEntityLength(long entityLength) { this.entityLength = entityLength; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public String getMetadata() { return metadata; }
    public void setMetadata(String metadata) { this.metadata = metadata; }

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

    public Instant getLastActivity() { return lastActivity; }
    public void setLastActivity(Instant lastActivity) { this.lastActivity = lastActivity; }

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

    public String toJson() {
        var builder = Json.createObjectBuilder()
                .add("entityLength", entityLength)
                .add("offset", offset)
                .add("isPartial", isPartial)
                .add("deferredLength", deferredLength)
                .add("isFinalConcat", isFinalConcat);

        if (metadata != null) {
            builder.add("metadata", metadata);
        }
        if (uploadConcatMergedValue != null) {
            builder.add("uploadConcatMergedValue", uploadConcatMergedValue);
        }
        if (expiresAt != null) {
            builder.add("expiresAt", expiresAt.toString());
        }
        if (partialIds != null) {
            JsonArrayBuilder arr = Json.createArrayBuilder();
            partialIds.forEach(arr::add);
            builder.add("partialIds", arr);
        }
        if (uploaderId != null) {
            builder.add("uploaderId", uploaderId);
        }
        if (lastActivity != null) {
            builder.add("lastActivity", lastActivity.toString());
        }

        return builder.build().toString();
    }

    public static UploadInfo fromJson(String json) {
        try (JsonReader reader = Json.createReader(new StringReader(json))) {
            JsonObject obj = reader.readObject();
            UploadInfo info = new UploadInfo();
            info.setEntityLength(obj.getJsonNumber("entityLength").longValue());
            info.setOffset(obj.getJsonNumber("offset").longValue());
            info.setPartial(obj.getBoolean("isPartial", false));
            info.setDeferredLength(obj.getBoolean("deferredLength", false));
            info.setFinalConcat(obj.getBoolean("isFinalConcat", false));

            if (obj.containsKey("metadata") && !obj.isNull("metadata")) {
                info.setMetadata(obj.getString("metadata"));
            }
            if (obj.containsKey("uploadConcatMergedValue") && !obj.isNull("uploadConcatMergedValue")) {
                info.setUploadConcatMergedValue(obj.getString("uploadConcatMergedValue"));
            }
            if (obj.containsKey("expiresAt") && !obj.isNull("expiresAt")) {
                info.setExpiresAt(Instant.parse(obj.getString("expiresAt")));
            }
            if (obj.containsKey("partialIds") && !obj.isNull("partialIds")) {
                List<String> ids = new ArrayList<>();
                obj.getJsonArray("partialIds").forEach(v -> ids.add(((JsonString) v).getString()));
                info.setPartialIds(ids);
            }
            if (obj.containsKey("uploaderId") && !obj.isNull("uploaderId")) {
                info.setUploaderId(obj.getString("uploaderId"));
            }
            if (obj.containsKey("lastActivity") && !obj.isNull("lastActivity")) {
                info.setLastActivity(Instant.parse(obj.getString("lastActivity")));
            }

            return info;
        }
    }

    @Override
    public String toString() {
        return "UploadInfo{" +
                "entityLength=" + entityLength +
                ", offset=" + offset +
                ", metadata='" + metadata + '\'' +
                ", isPartial=" + isPartial +
                '}';
    }
}
