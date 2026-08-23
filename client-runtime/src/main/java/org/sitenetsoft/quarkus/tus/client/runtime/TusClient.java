package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.vertx.core.buffer.Buffer;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusCapabilityException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusChecksumMismatchException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusOffsetMismatchException;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusServerErrorException;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusServerCapabilities;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUpload;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadProgress;
import org.sitenetsoft.quarkus.tus.client.runtime.model.TusUploadResult;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * High-level TUS client: capability discovery, creation, and the chunked upload loop, on top of the
 * low-level {@link TusProtocolClient}.
 *
 * <p><strong>Scope (Task 7):</strong> the sequential happy path — one chunk after another, in order,
 * over a single connection. Checksum digesting, defer-length and parallel upload are later tasks;
 * requesting them here fails fast with {@link UnsupportedOperationException} rather than silently
 * ignoring the option.
 *
 * <p><strong>Retry/resume (Task 8):</strong> a chunk PATCH that fails with a retryable error — a 5xx
 * ({@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusServerErrorException}), a 409 offset
 * conflict ({@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusOffsetMismatchException}), a
 * 460 checksum mismatch
 * ({@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusChecksumMismatchException}), or any
 * failure that isn't a {@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException} at
 * all (treated as an I/O-level failure, e.g. a reset connection) — is retried, provided the source is
 * {@link UploadSource#replayable()}: the loop waits {@code min(retryBackoff * 2^attempt,
 * retryBackoffMax)} (via {@code Uni.onItem().delayIt()}, never a blocking sleep), re-resolves the true
 * offset with a HEAD ({@link TusProtocolClient#offset(String)}), and resumes the chunk loop from
 * there. Every other {@code TusClientException} (4xx client errors like 413, protocol errors, etc.)
 * fails fast with no retry. A retryable failure against a non-replayable source fails immediately with
 * a message naming that. Attempts are consecutive-failure counters: a successful chunk resets the
 * counter to zero, so {@code maxRetries} bounds a run of consecutive failures, not the whole upload.
 * The initial {@code create()} call itself is not retried by this loop — a failure there propagates
 * as-is.
 */
public class TusClient {

    private static final Logger LOG = Logger.getLogger(TusClient.class);

    private final TusClientOptions options;
    private final TusProtocolClient protocol;
    private final Uni<org.sitenetsoft.quarkus.tus.client.runtime.model.TusServerCapabilities> capabilities;

    private TusClient(TusClientOptions options, TusProtocolClient protocol) {
        this.options = options;
        this.protocol = protocol;
        // Cached per client instance: the first upload's options() call primes it, every later
        // upload on this client reuses the result instead of re-fetching capabilities.
        this.capabilities = protocol.options().memoize().indefinitely();
    }

    public static TusClient create(io.vertx.core.Vertx vertx, TusClientOptions options) {
        TusTarget.Builder targetBuilder = TusTarget.builder(options.url());
        options.connectTimeout().ifPresent(targetBuilder::connectTimeout);
        options.requestTimeout().ifPresent(targetBuilder::requestTimeout);
        options.customizer().ifPresent(targetBuilder::customizer);
        TusProtocolClient protocol = new TusProtocolClient(vertx, targetBuilder.build());
        return new TusClient(options, protocol);
    }

    public TusProtocolClient protocol() {
        return protocol;
    }

    public Uni<TusUploadResult> upload(TusUploadRequest request) {
        int parallelism = request.parallelism().orElse(options.parallelism());
        UploadSource source = request.source();

        if (parallelism > 1) {
            // Both checked synchronously, before any request goes out -- same rationale as the
            // checksum/replayable guard below. The third guard (the "concatenation" extension itself)
            // can only be known after an OPTIONS round trip, so it's checked inside the capabilities
            // flatMap alongside the other capability checks, not here.
            if (!source.replayable()) {
                throw new TusClientException(
                        "parallelism > 1 requires a replayable source, but this source is not "
                                + "replayable (one-shot sources cannot be split into concurrently "
                                + "uploaded ranges)");
            }
            if (source.length() < 0) {
                throw new TusClientException(
                        "parallelism > 1 requires a known length, but this source's length is not "
                                + "known up front (defer-length and parallel upload are mutually "
                                + "exclusive)");
            }
        }

        String checksumAlgorithm = request.checksumAlgorithm().orElse(options.checksumAlgorithm().orElse(null));
        if (checksumAlgorithm != null) {
            if (!source.replayable()) {
                // Checksumming re-reads the chunk to digest it before sending, and a mismatch is
                // retried by re-sending the same chunk (Task 8) -- neither is possible against a
                // source that can only be consumed once. Fail fast, synchronously, before any request
                // goes out, rather than discovering this mid-upload.
                throw new TusClientException(
                        "checksumAlgorithm requires a replayable source, but this source is not "
                                + "replayable (one-shot sources cannot be re-read to be digested or retried)");
            }
            // Validated eagerly (not deferred into the capability-check flatMap below) so an unknown
            // algorithm name fails at request time with a clear, typed error instead of surfacing
            // later as an opaque NoSuchAlgorithmException from inside the chunk loop.
            javaDigestName(checksumAlgorithm);
        }

        long length = source.length();
        long chunkSize = request.chunkSize().orElse(options.chunkSize());
        Consumer<TusUploadProgress> onProgress = request.onProgress().orElse(null);

        if (parallelism > 1) {
            return capabilities.flatMap(caps -> {
                requireChecksumCapability(caps, checksumAlgorithm);
                requireConcatenationCapability(caps);
                return uploadParallel(source, length, parallelism, chunkSize, request.metadata(), onProgress,
                        checksumAlgorithm);
            });
        }

        boolean deferLength = length < 0;
        TusCreateOptions createOptions = deferLength
                ? TusCreateOptions.builder().deferLength().metadata(request.metadata()).build()
                : TusCreateOptions.builder().length(length).metadata(request.metadata()).build();

        return capabilities
                .flatMap(caps -> {
                    requireChecksumCapability(caps, checksumAlgorithm);
                    if (deferLength) {
                        requireDeferLengthCapability(caps);
                    }
                    return protocol.create(createOptions);
                })
                .flatMap(created -> deferLength
                        ? uploadDeferLength(created, source, chunkSize, onProgress)
                                .map(finalOffset -> new TusUploadResult(created.url(), finalOffset))
                        : uploadRange(created, source, created.offset(), length, chunkSize, onProgress,
                                checksumAlgorithm)
                                .map(finalOffset -> new TusUploadResult(created.url(), finalOffset)));
    }

    /**
     * One contiguous byte range of a parallel upload, and the index it must be placed back at (in the
     * source's original order) once all ranges have finished uploading concurrently and in whatever
     * order they happen to complete.
     */
    private record PartialRange(int index, long from, long to) {
        long length() {
            return to - from;
        }
    }

    /**
     * Splits {@code [0, length)} into {@code parallelism} contiguous, non-overlapping ranges; the
     * last range absorbs whatever remainder doesn't divide evenly.
     */
    private static List<PartialRange> splitRanges(long length, int parallelism) {
        List<PartialRange> ranges = new ArrayList<>(parallelism);
        // Ceiling division: every range but the last is exactly `base` bytes, and the last one takes
        // whatever's left over (equal to `base` when length divides evenly, smaller otherwise -- e.g.
        // 11 bytes over 3 ranges is 4, 4, 3, not 3, 3, 5).
        long base = (length + parallelism - 1) / parallelism;
        long from = 0;
        for (int i = 0; i < parallelism; i++) {
            long to = (i == parallelism - 1) ? length : Math.min(from + base, length);
            ranges.add(new PartialRange(i, from, to));
            from = to;
        }
        return ranges;
    }

    /**
     * Uploads {@code source} as {@code parallelism} concurrently-uploaded TUS partial uploads, then
     * concatenates them (in source-range order) into one final upload. Each partial is created with
     * {@code Upload-Concat: partial} and no metadata -- the request's metadata is carried only by the
     * final concatenating creation, per the TUS concatenation extension. Each partial gets its own
     * retry budget: {@link #uploadRange} (via {@link #attemptFromOffset}) is reused unchanged per
     * partial, so a transient failure on one partial doesn't spend down another partial's retries.
     * Bounded to {@code parallelism} concurrent partials at a time; the final concatenate lists the
     * partials' URLs in their original range order regardless of which one finished uploading first.
     *
     * <p><strong>Fail-fast:</strong> {@code merge} propagates the first partial's failure as soon as
     * it occurs and cancels every other still-in-flight partial's {@code Uni} (each of which cancels
     * its own in-progress HTTP request in turn) rather than waiting for the remaining partials to
     * finish first -- one partial exhausting its retry budget ends the whole parallel upload promptly
     * instead of after every sibling partial has also run to completion or exhaustion.
     */
    private Uni<TusUploadResult> uploadParallel(UploadSource source, long length, int parallelism, long chunkSize,
            Map<String, String> metadata, Consumer<TusUploadProgress> onProgress, String checksumAlgorithm) {
        List<PartialRange> ranges = splitRanges(length, parallelism);
        AtomicLong totalConfirmed = new AtomicLong(0);

        return Multi.createFrom().iterable(ranges)
                .onItem().transformToUni(
                        range -> uploadPartial(source, range, chunkSize, totalConfirmed, length, onProgress,
                                checksumAlgorithm))
                .merge(parallelism)
                .collect().asList()
                .flatMap(indexedUrls -> {
                    List<String> orderedUrls = indexedUrls.stream()
                            .sorted(Comparator.comparingInt(IndexedUrl::index))
                            .map(IndexedUrl::url)
                            .toList();
                    TusCreateOptions concatOptions = TusCreateOptions.builder().metadata(metadata).build();
                    return protocol.concatenate(orderedUrls, concatOptions)
                            .map(finalUpload -> new TusUploadResult(finalUpload.url(), length));
                });
    }

    private record IndexedUrl(int index, String url) {
    }

    /**
     * Creates and fully uploads one partial (a {@link PartialRange} slice of {@code source}), through
     * the same retry-wrapped range loop the sequential path uses ({@link #uploadRange}). The slice is
     * exposed to that loop as a fresh, offset-shifted {@link UploadSource} so the loop can keep
     * addressing it as {@code [0, range.length())} exactly like a whole, non-parallel upload -- the
     * shift back to the range's true position in {@code source} happens once, in
     * {@link #rangeSource}.
     */
    private Uni<IndexedUrl> uploadPartial(UploadSource source, PartialRange range, long chunkSize,
            AtomicLong totalConfirmed, long totalLength, Consumer<TusUploadProgress> onProgress,
            String checksumAlgorithm) {
        UploadSource slice = rangeSource(source, range.from(), range.to());
        TusCreateOptions partialCreateOptions = TusCreateOptions.builder().length(range.length()).partial().build();
        AtomicLong lastReportedForThisPartial = new AtomicLong(0);
        Consumer<TusUploadProgress> aggregatedProgress = onProgress == null
                ? null
                : progress -> {
                    long delta = progress.bytesSent() - lastReportedForThisPartial.getAndSet(progress.bytesSent());
                    long overall = totalConfirmed.addAndGet(delta);
                    reportProgress(onProgress, overall, totalLength);
                };
        return protocol.create(partialCreateOptions)
                .flatMap(created -> uploadRange(created, slice, created.offset(), range.length(), chunkSize,
                        aggregatedProgress, checksumAlgorithm)
                        .map(finalOffset -> new IndexedUrl(range.index(), created.url())));
    }

    /**
     * An offset-shifted view of {@code base} covering only {@code [from, to)}: {@code length()}
     * reports the range's own length rather than the whole source's, and {@code slice(fromOffset)}
     * translates a range-relative offset back into {@code base}'s absolute one. This is what lets
     * {@link #uploadRange}/{@link #uploadChunks} -- written against a single, whole-source upload --
     * drive one partial's range without any change to that loop.
     */
    private static UploadSource rangeSource(UploadSource base, long from, long to) {
        return new UploadSource() {
            @Override
            public long length() {
                return to - from;
            }

            @Override
            public Multi<Buffer> slice(long fromOffset) {
                return base.slice(from + fromOffset);
            }

            @Override
            public boolean replayable() {
                return base.replayable();
            }
        };
    }

    /**
     * Requires the server to advertise {@code concatenation}, needed whenever {@code parallelism > 1}
     * (parallel upload is implemented as several partial uploads joined by the concatenation
     * extension).
     */
    private void requireConcatenationCapability(TusServerCapabilities caps) {
        if (!caps.supports("concatenation")) {
            throw new TusCapabilityException(
                    "Server does not advertise the concatenation extension, required for parallel "
                            + "uploads (parallelism > 1)",
                    "concatenation");
        }
    }

    /**
     * Requires the server to advertise {@code creation-defer-length}, needed whenever the source's
     * length isn't known up front.
     */
    private void requireDeferLengthCapability(TusServerCapabilities caps) {
        if (!caps.supports("creation-defer-length")) {
            throw new TusCapabilityException(
                    "Server does not advertise the creation-defer-length extension, required for "
                            + "uploads whose length is not known up front",
                    "creation-defer-length");
        }
    }

    /**
     * Requires the server to advertise both the {@code checksum} extension and the requested
     * algorithm, if a checksum algorithm was requested. A no-op when {@code checksumAlgorithm} is
     * {@code null}.
     */
    private void requireChecksumCapability(TusServerCapabilities caps, String checksumAlgorithm) {
        if (checksumAlgorithm == null) {
            return;
        }
        if (!caps.supports("checksum") || !caps.checksumAlgorithms().contains(checksumAlgorithm)) {
            throw new TusCapabilityException(
                    "Server does not support checksum algorithm '" + checksumAlgorithm
                            + "' (checksum extension and/or that algorithm not advertised)",
                    "checksum");
        }
    }

    /**
     * Maps a TUS wire algorithm name to the {@link MessageDigest} algorithm name Java understands.
     * An unrecognized name is treated the same as a server that doesn't support it: a
     * {@link TusCapabilityException} naming {@code checksum}.
     */
    private static String javaDigestName(String tusAlgorithm) {
        String javaName = Map.of("sha1", "SHA-1", "sha256", "SHA-256", "md5", "MD5").get(tusAlgorithm);
        if (javaName == null) {
            throw new TusCapabilityException("Unsupported checksum algorithm: " + tusAlgorithm, "checksum");
        }
        return javaName;
    }

    /**
     * Uploads {@code [from, to)} of {@code source} to {@code upload}'s URL.
     *
     * <p>This is the single retry-budget entry point: it wraps one "attempt" — a run of
     * {@link #uploadChunks} over the whole remaining range — in exactly one failure handler per
     * attempt, sharing a single mutable {@code attempt} counter across every retry. That single
     * shared counter is the fix for the bug where a per-chunk-recursion-frame retry handler let a
     * failure from a later chunk re-enter every earlier chunk's already-succeeded frame, each with
     * its own fresh budget — multiplying the effective retry count by the number of chunks that had
     * already succeeded. {@code uploadChunks} itself carries no failure handler of its own: a chunk
     * failure propagates straight up through every earlier chunk's {@code flatMap} to this one
     * recovery point, undecorated, no matter how many chunks preceded it.
     */
    private Uni<Long> uploadRange(TusUpload upload, UploadSource source, long from, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress, String checksumAlgorithm) {
        return attemptFromOffset(upload, source, from, to, chunkSize, onProgress,
                new AtomicLong(from), new AtomicInteger(0), checksumAlgorithm);
    }

    /**
     * One attempt: upload the remaining range starting at {@code from}, and if it fails
     * retryably, hand off to {@link #retryFromOffset}. {@code confirmedOffset} and {@code attempt}
     * are shared, mutable, and updated in place by {@link #uploadChunks} as chunks succeed — they are
     * NOT re-created per attempt, which is what keeps the retry budget scoped to the whole upload
     * rather than to whichever recursion frame happens to catch the failure.
     */
    private Uni<Long> attemptFromOffset(TusUpload upload, UploadSource source, long from, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress, AtomicLong confirmedOffset,
            AtomicInteger attempt, String checksumAlgorithm) {
        return uploadChunks(upload, source, from, to, chunkSize, onProgress, confirmedOffset, attempt, checksumAlgorithm)
                .onFailure(this::isRetryable)
                .recoverWithUni(failure -> retryFromOffset(upload, source, to, chunkSize, onProgress,
                        confirmedOffset, attempt, failure, checksumAlgorithm));
    }

    /**
     * Uploads {@code [from, to)} one {@code chunkSize} slice at a time, as a recursive {@code Uni}
     * chain with NO failure handler attached anywhere in the recursion — a failure on any chunk
     * propagates, undecorated, straight up to whichever single caller attached a handler (that's
     * {@link #attemptFromOffset}, exactly once per attempt). Kept range-capable (rather than always
     * starting at 0 and always running to the source's full length) so Task 11's partial-upload
     * concatenation can reuse it for an arbitrary sub-range.
     */
    private Uni<Long> uploadChunks(TusUpload upload, UploadSource source, long from, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress, AtomicLong confirmedOffset,
            AtomicInteger attempt, String checksumAlgorithm) {
        if (from >= to) {
            return Uni.createFrom().item(from);
        }
        long end = Math.min(from + chunkSize, to);
        long len = end - from;
        Multi<Buffer> limited = BufferLimiter.limit(source.slice(from), len);

        Uni<Long> patchCall = checksumAlgorithm == null
                ? protocol.patch(upload.url(), from, limited, TusPatchOptions.builder().contentLength(len).build())
                : digestAndPatch(upload, from, len, limited, checksumAlgorithm);

        return patchCall
                .invoke(newOffset -> {
                    reportProgress(onProgress, newOffset, to);
                    confirmedOffset.set(newOffset);
                    // A successful chunk resets the attempt counter: only *consecutive* failures
                    // count against maxRetries, so a long upload that hits one transient error every
                    // so often keeps making progress instead of eventually exhausting its retry
                    // budget on unrelated, already-recovered-from failures.
                    attempt.set(0);
                })
                .flatMap(newOffset -> uploadChunks(upload, source, newOffset, to, chunkSize, onProgress,
                        confirmedOffset, attempt, checksumAlgorithm));
    }

    /**
     * The checksum path: unlike the pure-streaming path above, this collects the whole chunk into one
     * {@link Buffer} before sending it. That's a deliberate, bounded buffer — bounded because
     * {@code limited} is already capped at {@code len} (at most one {@code chunkSize}) by
     * {@link BufferLimiter} — not an accidental whole-upload buffering: it's the only way to compute a
     * digest of the chunk before the PATCH that carries it, since the TUS checksum extension puts the
     * digest in a request header rather than a trailer. The collected buffer is then sent as a
     * single-item {@code Multi}.
     */
    private Uni<Long> digestAndPatch(TusUpload upload, long from, long len, Multi<Buffer> limited,
            String checksumAlgorithm) {
        return limited.collect().in(Buffer::buffer, Buffer::appendBuffer)
                .flatMap(collected -> {
                    String digest = digestBase64(checksumAlgorithm, collected);
                    TusPatchOptions patchOptions = TusPatchOptions.builder()
                            .contentLength(len)
                            .checksum(checksumAlgorithm, digest)
                            .build();
                    Multi<Buffer> singleItem = Multi.createFrom().item(collected);
                    return protocol.patch(upload.url(), from, singleItem, patchOptions);
                });
    }

    private static String digestBase64(String checksumAlgorithm, Buffer data) {
        try {
            MessageDigest digest = MessageDigest.getInstance(javaDigestName(checksumAlgorithm));
            return Base64.getEncoder().encodeToString(digest.digest(data.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            // javaDigestName() already validated the name maps to one of three algorithm names Java
            // is guaranteed to provide (SHA-1, SHA-256, MD5 are all in every standard JDK), so this
            // can't actually happen; kept as a typed failure rather than letting a checked exception
            // force an unchecked wrapper at every call site.
            throw new TusCapabilityException("Unsupported checksum algorithm: " + checksumAlgorithm, "checksum");
        }
    }

    /**
     * Uploads a one-shot, unknown-length source under the {@code creation-defer-length} extension:
     * the upload was created with {@code Upload-Defer-Length: 1} and no {@code Upload-Length}, and the
     * true length is announced only once the source itself runs out of bytes.
     *
     * <p>The source's single {@code slice(0)} {@code Multi} is pulled with manual, PATCH-paced demand
     * (one upstream {@code request(1)} at a time) rather than eagerly drained: bytes are accumulated
     * into a buffer bounded at {@code chunkSize}, and as soon as that buffer holds a full chunk it is
     * PATCHed before any more upstream data is requested, so memory use never exceeds one chunk no
     * matter how fast the source produces data or how slow the server is to accept it. There is no
     * retry here (a one-shot source cannot be replayed) and no capability check for checksums, since
     * a checksum requires a replayable source and that guard already fired earlier in {@link #upload}.
     *
     * <p>Once the source completes, any leftover partial chunk is flushed as an ordinary data PATCH,
     * and then — always, even for a completely empty source — one final, empty-bodied PATCH declares
     * {@code Upload-Length} as the confirmed final offset, per the extension's contract.
     */
    private Uni<Long> uploadDeferLength(TusUpload upload, UploadSource source, long chunkSize,
            Consumer<TusUploadProgress> onProgress) {
        return Uni.createFrom().emitter(emitter -> {
            AtomicLong offset = new AtomicLong(upload.offset());
            AtomicReference<Buffer> pending = new AtomicReference<>(Buffer.buffer());
            AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();

            source.slice(0).subscribe().withSubscriber(new MultiSubscriber<Buffer>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscriptionRef.set(subscription);
                    subscription.request(1);
                }

                @Override
                public void onItem(Buffer item) {
                    pending.get().appendBuffer(item);
                    drainFullChunks();
                }

                @Override
                public void onFailure(Throwable failure) {
                    // The upstream itself failed -- it has already terminated on its own, so there's
                    // no live subscription left to cancel here. (Kept symmetrical with sendChunk's
                    // failure path below rather than assuming that invariant silently.)
                    cancelUpstream();
                    emitter.fail(failure);
                }

                @Override
                public void onCompletion() {
                    flushRemainderThenDeclare();
                }

                /**
                 * Releases the source's underlying resource on any path that ends the upload without
                 * the upstream {@code Multi} having reached its own natural completion. The
                 * declarative {@code Uni}/{@code Multi} pipelines elsewhere in this class cancel their
                 * upstream automatically on failure or downstream cancellation; this hand-rolled
                 * subscriber has to do that itself, or a source like {@link
                 * org.sitenetsoft.quarkus.tus.client.runtime.source.FileUploadSource} would leak an
                 * open file handle every time a chunk PATCH failed mid-stream.
                 */
                private void cancelUpstream() {
                    Flow.Subscription subscription = subscriptionRef.get();
                    if (subscription != null) {
                        subscription.cancel();
                    }
                }

                private void drainFullChunks() {
                    Buffer buffered = pending.get();
                    if (buffered.length() < chunkSize) {
                        subscriptionRef.get().request(1);
                        return;
                    }
                    Buffer chunk = buffered.getBuffer(0, (int) chunkSize);
                    pending.set(buffered.getBuffer((int) chunkSize, buffered.length()));
                    sendChunk(chunk, chunkSize, this::drainFullChunks);
                }

                private void flushRemainderThenDeclare() {
                    Buffer remainder = pending.get();
                    if (remainder.length() > 0) {
                        pending.set(Buffer.buffer());
                        sendChunk(remainder, remainder.length(), this::declareFinalLength);
                    } else {
                        declareFinalLength();
                    }
                }

                private void sendChunk(Buffer chunk, long len, Runnable onSuccess) {
                    protocol.patch(upload.url(), offset.get(), Multi.createFrom().item(chunk),
                            TusPatchOptions.builder().contentLength(len).build())
                            .subscribe().with(newOffset -> {
                                reportProgress(onProgress, newOffset, -1);
                                offset.set(newOffset);
                                onSuccess.run();
                            }, failure -> {
                                // A failed chunk PATCH ends the upload right here (no retry -- the
                                // source isn't replayable), so the still-open upstream subscription
                                // needs to be told to release its resource explicitly; nothing else
                                // will drain or cancel it now.
                                cancelUpstream();
                                emitter.fail(failure);
                            });
                }

                private void declareFinalLength() {
                    long finalOffset = offset.get();
                    // No body Multi at all (not even an empty one): an empty Multi completes its
                    // subscription synchronously, and smallrye-mutiny-vertx's ReadStreamSubscriber
                    // throws IllegalStateException if endHandler(...) is attached after onComplete()
                    // has already fired -- exactly what happens when Vert.x's own send()/pipeTo sets
                    // that handler a moment later. Passing null skips that pipe entirely, so the
                    // .contentLength(0) below never reaches the wire (there's no body to measure) --
                    // it's kept only for readability, documenting that this PATCH's body is empty.
                    // Vert.x sends a plain bodyless PATCH (Content-Length: 0) on its own.
                    protocol.patch(upload.url(), finalOffset, null,
                            TusPatchOptions.builder().contentLength(0).declareUploadLength(finalOffset).build())
                            .subscribe().with(emitter::complete, emitter::fail);
                }
            });
        });
    }

    /**
     * Handles one retryable failure of the current attempt: gives up (propagating {@code failure})
     * if the source can't be replayed or the retry budget is exhausted, otherwise backs off, resyncs
     * the true offset with a HEAD, and starts a fresh attempt from there.
     */
    private Uni<Long> retryFromOffset(TusUpload upload, UploadSource source, long to, long chunkSize,
            Consumer<TusUploadProgress> onProgress, AtomicLong confirmedOffset,
            AtomicInteger attempt, Throwable failure, String checksumAlgorithm) {
        if (!source.replayable()) {
            return Uni.createFrom().failure(new TusClientException(
                    "Upload failed and the source is not replayable, so it cannot be resumed: "
                            + failure.getMessage(),
                    failure));
        }
        int priorAttempt = attempt.get();
        int nextAttempt = attempt.incrementAndGet();
        if (nextAttempt > options.maxRetries()) {
            return Uni.createFrom().failure(failure);
        }
        LOG.debugf(failure, "TUS chunk upload failed (attempt %d/%d), backing off and resyncing offset",
                nextAttempt, options.maxRetries());
        return Uni.createFrom().voidItem()
                .onItem().delayIt().by(backoffFor(priorAttempt))
                .flatMap(ignored -> protocol.offset(upload.url()))
                .invoke(confirmedOffset::set)
                .flatMap(resyncedOffset -> attemptFromOffset(upload, source, resyncedOffset, to, chunkSize,
                        onProgress, confirmedOffset, attempt, checksumAlgorithm));
    }

    /**
     * {@code min(retryBackoff * 2^attempt, retryBackoffMax)}. {@code attempt} is the number of prior
     * consecutive failures (0 for the delay before the first retry), so the wait doubles on each
     * further consecutive failure.
     */
    private Duration backoffFor(int attempt) {
        long multiplier = 1L << Math.min(attempt, 30);
        Duration backoff = options.retryBackoff().multipliedBy(multiplier);
        Duration max = options.retryBackoffMax();
        return backoff.compareTo(max) > 0 ? max : backoff;
    }

    /**
     * Retryable: a 5xx, a 409 offset conflict, a 460 checksum mismatch, or anything that isn't even a
     * {@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException} (treated as an
     * I/O-level failure). Every other {@code TusClientException} — 4xx client errors, protocol
     * errors, etc. — fails fast.
     */
    private boolean isRetryable(Throwable failure) {
        return failure instanceof TusServerErrorException
                || failure instanceof TusOffsetMismatchException
                || failure instanceof TusChecksumMismatchException
                || !(failure instanceof TusClientException);
    }

    private void reportProgress(Consumer<TusUploadProgress> onProgress, long bytesSent, long total) {
        if (onProgress == null) {
            return;
        }
        try {
            onProgress.accept(new TusUploadProgress(bytesSent, total));
        } catch (RuntimeException e) {
            // A misbehaving progress callback must not break the upload itself, but it shouldn't
            // vanish silently either.
            LOG.debugf(e, "TusUploadRequest onProgress callback threw for bytesSent=%d, totalBytes=%d",
                    bytesSent, total);
        }
    }

    /**
     * Closes the underlying HTTP client. Same event-loop caveat as {@link TusProtocolClient#close()}.
     */
    public void close() {
        protocol.close();
    }
}
