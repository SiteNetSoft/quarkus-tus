package org.sitenetsoft.quarkus.tus.client.runtime.model;

/**
 * The outcome of a completed (or resumed-to-completion) TUS upload.
 *
 * @param url the upload resource URL
 * @param bytesUploaded the total number of bytes confirmed by the server (the final offset)
 */
public record TusUploadResult(String url, long bytesUploaded) {
}
