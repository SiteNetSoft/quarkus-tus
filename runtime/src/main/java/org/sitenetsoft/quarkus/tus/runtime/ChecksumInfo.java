package org.sitenetsoft.quarkus.tus.runtime;

/** A parsed {@code Upload-Checksum} header: the algorithm name and the Base64 digest. */
public record ChecksumInfo(String algorithm, String value) {
}
