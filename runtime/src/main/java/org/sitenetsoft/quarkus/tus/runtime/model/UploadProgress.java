package org.sitenetsoft.quarkus.tus.runtime.model;

public class UploadProgress {

    public final long totalBytes;
    public long uploadedBytes;

    public UploadProgress(long totalBytes) {
        this.totalBytes = totalBytes;
        this.uploadedBytes = 0L;
    }

    public int getPercentage() {
        if (totalBytes <= 0) {
            return 0;
        }
        long pct = (uploadedBytes * 100L) / totalBytes;
        if (pct < 0L) pct = 0L;
        if (pct > 100L) pct = 100L;
        return (int) pct;
    }
}
