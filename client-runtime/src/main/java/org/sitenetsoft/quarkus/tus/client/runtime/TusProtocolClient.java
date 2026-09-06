package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.http.HttpClient;
import io.vertx.mutiny.core.http.HttpClientResponse;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusProtocolException;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusServerCapabilities;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUpload;

import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Speaks the base TUS protocol plus the creation and creation-with-upload extensions over HTTP.
 */
public class TusProtocolClient {

    private static final String TUS_RESUMABLE = "1.0.0";

    private final TusTarget target;
    private final HttpClient httpClient;

    public TusProtocolClient(io.vertx.core.Vertx vertx, TusTarget target) {
        this.target = target;
        HttpClientOptions options = new HttpClientOptions();
        target.connectTimeout().ifPresent(d -> options.setConnectTimeout((int) d.toMillis()));
        // Last, so the hook can override anything set above.
        target.httpClientOptions().ifPresent(hook -> hook.customize(options));
        this.httpClient = Vertx.newInstance(vertx).createHttpClient(options);
    }

    public Uni<TusServerCapabilities> options() {
        // The spec allows a 200 or a 204 for OPTIONS; tusd answers 200, this project's server 204.
        return request(HttpMethod.OPTIONS, target.url(), MultiMap.caseInsensitiveMultiMap(), null, -1, Set.of(200, 204))
                .map(this::toCapabilities);
    }

    public Uni<TusUpload> create(TusCreateOptions opts) {
        MultiMap headers = MultiMap.caseInsensitiveMultiMap();
        if (opts.length().isPresent()) {
            headers.set("Upload-Length", Long.toString(opts.length().getAsLong()));
        }
        if (opts.deferLength()) {
            headers.set("Upload-Defer-Length", "1");
        }
        if (!opts.metadata().isEmpty()) {
            headers.set("Upload-Metadata", TusClientUtils.encodeMetadata(opts.metadata()));
        }
        if (opts.partial()) {
            headers.set("Upload-Concat", "partial");
        }

        Multi<io.vertx.core.buffer.Buffer> body = opts.body().orElse(null);
        long bodyLength = opts.body().isPresent() ? opts.bodyLength() : -1;
        if (opts.body().isPresent()) {
            headers.set("Content-Type", "application/offset+octet-stream");
        }

        long declaredLength = opts.length().isPresent() ? opts.length().getAsLong() : -1L;
        return request(HttpMethod.POST, target.url(), headers, body, bodyLength, Set.of(201))
                .map(resp -> toUpload(resp, declaredLength));
    }

    public Uni<Long> offset(String uploadUrl) {
        return request(HttpMethod.HEAD, uploadUrl, MultiMap.caseInsensitiveMultiMap(), null, -1, Set.of(200))
                .map(this::readUploadOffset);
    }

    public Uni<Long> patch(String uploadUrl, long offset, Multi<io.vertx.core.buffer.Buffer> data, TusPatchOptions opts) {
        MultiMap headers = MultiMap.caseInsensitiveMultiMap();
        headers.set("Content-Type", "application/offset+octet-stream");
        headers.set("Upload-Offset", Long.toString(offset));
        if (opts.checksumAlgorithm().isPresent()) {
            // TusPatchOptions.checksum(algorithm, digest) always sets both together, so the digest
            // is guaranteed present here; fail fast instead of silently sending a blank digest.
            headers.set("Upload-Checksum", opts.checksumAlgorithm().get() + " " + opts.checksumDigest().orElseThrow());
        }
        if (opts.declaredUploadLength().isPresent()) {
            headers.set("Upload-Length", Long.toString(opts.declaredUploadLength().getAsLong()));
        }
        long bodyLength = opts.contentLength().isPresent() ? opts.contentLength().getAsLong() : -1;

        return request(HttpMethod.PATCH, uploadUrl, headers, data, bodyLength, Set.of(200, 204))
                .map(this::readUploadOffset);
    }

    public Uni<Void> terminate(String uploadUrl) {
        return request(HttpMethod.DELETE, uploadUrl, MultiMap.caseInsensitiveMultiMap(), null, -1, Set.of(204))
                .replaceWithVoid();
    }

    public Uni<TusUpload> concatenate(List<String> partialUrls, TusCreateOptions opts) {
        MultiMap headers = MultiMap.caseInsensitiveMultiMap();
        headers.set("Upload-Concat", "final;" + String.join(" ", partialUrls));
        if (!opts.metadata().isEmpty()) {
            headers.set("Upload-Metadata", TusClientUtils.encodeMetadata(opts.metadata()));
        }
        // Spec: the final creation request carries no Upload-Length, even if opts carries one.
        return request(HttpMethod.POST, target.url(), headers, null, -1, Set.of(201))
                .map(resp -> toUpload(resp, -1L));
    }

    private long readUploadOffset(HttpClientResponse resp) {
        String offsetHeader = resp.getHeader("Upload-Offset");
        if (offsetHeader == null) {
            throw new TusProtocolException("Missing required Upload-Offset header");
        }
        return Long.parseLong(offsetHeader);
    }

    /**
     * Closes the underlying HTTP client, releasing pooled connections.
     *
     * <p><strong>Must not be called from a Vert.x event-loop context</strong> (e.g. from inside a
     * handler running on the Vert.x context this client's {@code Vertx} belongs to): it blocks the
     * calling thread waiting for the close to complete, and blocking the event loop that the close
     * itself depends on deadlocks. Call it from a worker thread, a shutdown hook, or after leaving
     * the Vert.x context.
     */
    public void close() {
        httpClient.close().await().indefinitely();
    }

    private Uni<HttpClientResponse> request(HttpMethod method, String url, MultiMap headers,
            Multi<io.vertx.core.buffer.Buffer> body, long bodyLength, Set<Integer> expectedStatuses) {
        RequestOptions options = new RequestOptions()
                .setMethod(method)
                .setAbsoluteURI(url);
        target.requestTimeout().ifPresent(d -> options.setTimeout(d.toMillis()));

        headers.set("Tus-Resumable", TUS_RESUMABLE);
        target.customizer().ifPresent(c -> c.customize(method.name(), url, headers));

        return httpClient.request(options)
                .invoke(req -> headers.forEach(entry -> req.putHeader(entry.getKey(), entry.getValue())))
                .flatMap(req -> {
                    if (body != null) {
                        if (bodyLength >= 0) {
                            req.putHeader("Content-Length", Long.toString(bodyLength));
                        } else {
                            req.putHeader("Transfer-Encoding", "chunked");
                        }
                        Multi<io.vertx.mutiny.core.buffer.Buffer> mutinyBody =
                                body.map(io.vertx.mutiny.core.buffer.Buffer::newInstance);
                        return req.send(mutinyBody);
                    }
                    return req.send();
                })
                .flatMap(resp -> resp.body().map(ignored -> resp))
                .flatMap(resp -> {
                    if (expectedStatuses.contains(resp.statusCode())) {
                        return Uni.createFrom().item(resp);
                    }
                    boolean expired = resp.statusCode() == 410;
                    return Uni.createFrom().failure(TusClientUtils.fromStatus(resp.statusCode(), expired));
                });
    }

    private TusServerCapabilities toCapabilities(HttpClientResponse resp) {
        List<String> versions = splitCsv(resp.getHeader("Tus-Version"));
        Set<String> extensions = new HashSet<>(splitCsv(resp.getHeader("Tus-Extension")));
        Set<String> checksumAlgorithms = new HashSet<>(splitCsv(resp.getHeader("Tus-Checksum-Algorithm")));
        String maxSizeHeader = resp.getHeader("Tus-Max-Size");
        OptionalLong maxSize = maxSizeHeader == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(maxSizeHeader));
        return new TusServerCapabilities(versions, extensions, maxSize, checksumAlgorithms);
    }

    private TusUpload toUpload(HttpClientResponse resp, long declaredLength) {
        String location = resp.getHeader("Location");
        if (location == null) {
            throw new TusProtocolException("Missing required Location header in create response");
        }
        String resolvedUrl = resolveLocation(target.url(), location);

        String offsetHeader = resp.getHeader("Upload-Offset");
        long offset = offsetHeader == null ? 0L : Long.parseLong(offsetHeader);

        Optional<Instant> expiresAt = parseUploadExpires(resp.getHeader("Upload-Expires"));

        return new TusUpload(resolvedUrl, offset, declaredLength, expiresAt);
    }

    static String resolveLocation(String targetUrl, String location) {
        // Resolve as if the target URL names a directory: a relative Location (no leading "/")
        // is meant to sit alongside it, not replace its last path segment.
        String base = targetUrl.endsWith("/") ? targetUrl : targetUrl + "/";
        return URI.create(base).resolve(location).toString();
    }

    static Optional<Instant> parseUploadExpires(String header) {
        if (header == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(header)));
        } catch (DateTimeParseException e) {
            throw new TusProtocolException("Malformed Upload-Expires header: " + header);
        }
    }

    private static List<String> splitCsv(String header) {
        if (header == null || header.isBlank()) {
            return List.of();
        }
        return Arrays.stream(header.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
