package org.sitenetsoft.quarkus.tus.it;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStoreException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A streaming S3 {@link UploadStore} — the sample that proves the SPI lets bytes flow
 * client → server → object store without any store buffering a whole chunk. It lives in the test
 * module on purpose: the extension ships no vendor SDK, and this is what a consumer's own S3
 * store can start from. Runs against MinIO or any S3 at {@code $TUS_S3_ENDPOINT}.
 * <p>
 * Every upload is one S3 multipart upload. {@link #stageChunk} consumes the body stream and
 * uploads a part every {@link #PART_SIZE} bytes as they arrive (S3 refuses smaller non-final
 * parts), so memory is bounded by one part regardless of chunk size. Bytes beyond the last full
 * part are kept as a tail: a <em>staged</em> tail until commit, then a <em>committed</em> tail
 * that seeds the next stage. The last commit — the one that reaches {@code Upload-Length} —
 * uploads the tail as the final part and completes the multipart upload.
 * <p>
 * Records live in memory (the SPI's documented "local index" limitation) and are lost with the
 * process; the object store keeps only the bytes.
 */
@Singleton
@Alternative
public class S3UploadStore implements UploadStore {

    private static final Logger LOG = Logger.getLogger(S3UploadStore.class);
    static final long PART_SIZE = 5L * 1024 * 1024;

    private final Map<String, UploadInfo> records = new ConcurrentHashMap<>();
    private final Map<String, Multipart> multiparts = new ConcurrentHashMap<>();
    private final Set<String> locks = ConcurrentHashMap.newKeySet();

    /** Observability for the tests: how many parts went to S3, and the most bytes ever held. */
    public final AtomicLong partsUploaded = new AtomicLong();
    public final AtomicLong maxBufferedBytes = new AtomicLong();

    private S3AsyncClient s3;
    private String bucket;

    private static final class Multipart {
        String s3UploadId;                          // null once the object exists
        final List<CompletedPart> committedParts = new ArrayList<>();
        byte[] committedTail = new byte[0];         // committed, not yet a part
        List<CompletedPart> stagedParts;            // between stage and commit
        byte[] stagedTail;
        boolean complete;
    }

    // ---- lifecycle ----

    @PostConstruct
    void init() {
        String endpoint = System.getenv().getOrDefault("TUS_S3_ENDPOINT", "http://localhost:9000");
        bucket = System.getenv().getOrDefault("TUS_S3_BUCKET", "quarkus-tus");
        s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        System.getenv().getOrDefault("TUS_S3_ACCESS_KEY", "minio"),
                        System.getenv().getOrDefault("TUS_S3_SECRET_KEY", "minio12345"))))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).join();
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof BucketAlreadyOwnedByYouException)
                    && !(e.getCause() instanceof BucketAlreadyExistsException)) {
                throw e;
            }
        }
        LOG.infof("S3 sample store: %s, bucket %s", endpoint, bucket);
    }

    @PreDestroy
    void close() {
        s3.close();
    }

    public void resetMetrics() {
        partsUploaded.set(0);
        maxBufferedBytes.set(0);
    }

    // ---- records ----

    @Override
    public Optional<UploadInfo> findUploadInfo(String id) {
        return Optional.ofNullable(records.get(id));
    }

    @Override
    public String createUpload(UploadInfo info) {
        String id = UUID.randomUUID().toString();
        Multipart mp = new Multipart();
        try {
            if (info.getEntityLength() == 0) {
                // S3 cannot complete a multipart upload with no parts; an empty upload is an
                // empty object from the start.
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(id).build(),
                        AsyncRequestBody.empty()).join();
                mp.complete = true;
            } else {
                // The record methods are synchronous by SPI design, so this is a blocking
                // network call — the very limitation the SPI documents. Fine on a worker
                // (POST is @Blocking); a production store would keep this off the event loop.
                mp.s3UploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(bucket).key(id).build()).join().uploadId();
            }
        } catch (CompletionException e) {
            throw new UploadStoreException("Failed to start S3 upload for " + id, e.getCause());
        }
        multiparts.put(id, mp);
        records.put(id, info);
        return id;
    }

    @Override
    public void updateUploadInfo(String id, UploadInfo info) {
        if (records.containsKey(id)) {
            records.put(id, info);
        }
    }

    // ---- staged writes ----

    @Override
    public Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength) {
        UploadInfo info = records.get(id);
        Multipart mp = multiparts.get(id);
        if (info == null || mp == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(id));
        }
        if (offset != info.getOffset()) {
            return Uni.createFrom().failure(new OffsetMismatchException(
                    "Write at offset " + offset + " but upload " + id + " is at offset " + info.getOffset(),
                    info.getOffset()));
        }
        if (mp.complete) {
            return Uni.createFrom().failure(new UploadStoreException("Upload " + id + " is already complete"));
        }
        List<CompletedPart> stagedParts = new ArrayList<>();
        AtomicLong newBytes = new AtomicLong();
        return pump(id, mp, mp.committedTail, mp.committedParts.size(), data, stagedParts, newBytes)
                .onItem().transform(tail -> {
                    mp.stagedParts = stagedParts;
                    mp.stagedTail = tail;
                    return newBytes.get();
                });
    }

    /**
     * Streams {@code data} into parts of {@code PART_SIZE}, seeded with {@code seedTail}, starting
     * at part number {@code partsSoFar + 1}. Uploaded parts land in {@code partsOut}; the bytes
     * left over come back as the tail. {@code newBytes} counts only what came from {@code data}.
     * Failures from {@code data} propagate as they are.
     */
    private Uni<byte[]> pump(String id, Multipart mp, byte[] seedTail, int partsSoFar, Multi<Buffer> data,
                             List<CompletedPart> partsOut, AtomicLong newBytes) {
        ByteArrayOutputStream acc = new ByteArrayOutputStream();
        acc.writeBytes(seedTail);
        int[] nextPart = {partsSoFar + 1};
        return data
                .onItem().transformToUniAndConcatenate(buf -> {
                    acc.writeBytes(buf.getBytes());
                    newBytes.addAndGet(buf.length());
                    maxBufferedBytes.accumulateAndGet(acc.size(), Math::max);
                    if (acc.size() < PART_SIZE) {
                        return Uni.createFrom().voidItem();
                    }
                    byte[] part = acc.toByteArray();
                    acc.reset();
                    // Anything past a whole part carries over to the next one.
                    int whole = (int) (part.length - (part.length % PART_SIZE));
                    if (whole < part.length) {
                        acc.write(part, whole, part.length - whole);
                    }
                    return uploadPart(id, mp.s3UploadId, nextPart[0]++, Arrays.copyOf(part, whole))
                            .invoke(partsOut::add).replaceWithVoid();
                })
                .collect().last()
                .onItem().transform(v -> acc.toByteArray());
    }

    private Uni<CompletedPart> uploadPart(String id, String s3UploadId, int partNumber, byte[] bytes) {
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(bucket).key(id).uploadId(s3UploadId)
                .partNumber(partNumber).contentLength((long) bytes.length).build();
        return Uni.createFrom().completionStage(s3.uploadPart(request, AsyncRequestBody.fromBytes(bytes)))
                .invoke(() -> partsUploaded.incrementAndGet())
                .onItem().transform(resp -> CompletedPart.builder().partNumber(partNumber).eTag(resp.eTag()).build())
                .onFailure(CompletionException.class).transform(Throwable::getCause);
    }

    @Override
    public Uni<Void> commitChunk(String id, long offset, long bytesStaged) {
        UploadInfo info = records.get(id);
        Multipart mp = multiparts.get(id);
        if (info == null || mp == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(id));
        }
        if (mp.stagedParts != null) {
            mp.committedParts.addAll(mp.stagedParts);
            mp.committedTail = mp.stagedTail;
            mp.stagedParts = null;
            mp.stagedTail = null;
        }
        info.setOffset(offset + bytesStaged);
        info.setLastActivity(Instant.now());
        if (info.getEntityLength() >= 0 && info.getOffset() == info.getEntityLength() && !mp.complete) {
            return finish(id, mp);
        }
        return Uni.createFrom().voidItem();
    }

    /** Uploads the tail as the final part (S3 allows any size for the last one) and completes. */
    private Uni<Void> finish(String id, Multipart mp) {
        Uni<Void> tail = mp.committedTail.length > 0 || mp.committedParts.isEmpty()
                ? uploadPart(id, mp.s3UploadId, mp.committedParts.size() + 1, mp.committedTail)
                        .invoke(mp.committedParts::add).replaceWithVoid()
                : Uni.createFrom().voidItem();
        return tail.chain(() -> Uni.createFrom().completionStage(s3.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder().bucket(bucket).key(id).uploadId(mp.s3UploadId)
                                .multipartUpload(CompletedMultipartUpload.builder().parts(mp.committedParts).build())
                                .build())))
                .onFailure(CompletionException.class).transform(Throwable::getCause)
                .invoke(() -> {
                    mp.complete = true;
                    mp.committedTail = new byte[0];
                    mp.s3UploadId = null;
                })
                .replaceWithVoid();
    }

    @Override
    public Uni<Void> abortChunk(String id, long offset) {
        Multipart mp = multiparts.get(id);
        if (mp != null) {
            // Parts uploaded while staging are simply not listed at completion; S3 drops them.
            mp.stagedParts = null;
            mp.stagedTail = null;
        }
        return Uni.createFrom().voidItem();
    }

    // ---- concatenation ----

    @Override
    public Uni<Void> concatenate(String finalId, List<String> sourceIds) {
        UploadInfo finalInfo = records.get(finalId);
        Multipart mp = multiparts.get(finalId);
        if (finalInfo == null || mp == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(finalId));
        }
        for (String sourceId : sourceIds) {
            Multipart source = multiparts.get(sourceId);
            if (source == null || !source.complete) {
                return Uni.createFrom().failure(new UploadNotFoundException(sourceId));
            }
        }
        // Sources are streamed out of S3 and back in through the same part pump. UploadPartCopy
        // would avoid the round trip but shares S3's 5 MB rule for non-final parts, so small
        // partials could not be copied as parts anyway.
        Multi<Buffer> joined = Multi.createFrom().iterable(sourceIds)
                .onItem().transformToMultiAndConcatenate(this::objectBytes);
        List<CompletedPart> parts = new ArrayList<>();
        return pump(finalId, mp, new byte[0], 0, joined, parts, new AtomicLong())
                .chain(tail -> {
                    mp.committedParts.addAll(parts);
                    mp.committedTail = tail;
                    return finish(finalId, mp);
                })
                .invoke(() -> {
                    finalInfo.setOffset(finalInfo.getEntityLength());
                    finalInfo.setFinalConcat(false);
                    finalInfo.setPartialIds(null);
                    finalInfo.setLastActivity(Instant.now());
                })
                .onFailure().transform(e -> e instanceof UploadStoreException || e instanceof UploadNotFoundException
                        ? e : new UploadStoreException("Failed to concatenate into " + finalId, e));
    }

    private Multi<Buffer> objectBytes(String id) {
        return Uni.createFrom().completionStage(s3.getObject(
                        GetObjectRequest.builder().bucket(bucket).key(id).build(),
                        AsyncResponseTransformer.toPublisher()))
                .onItem().transformToMulti(publisher -> Multi.createFrom().publisher(AdaptersToFlow.publisher(publisher)))
                .onItem().transform(bb -> Buffer.buffer(io.netty.buffer.Unpooled.wrappedBuffer(bb)));
    }

    // ---- discard, locks, cleanup ----

    @Override
    public boolean discardUpload(String id) {
        UploadInfo removed = records.remove(id);
        Multipart mp = multiparts.remove(id);
        if (mp != null) {
            try {
                if (mp.s3UploadId != null) {
                    s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                            .bucket(bucket).key(id).uploadId(mp.s3UploadId).build()).join();
                }
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(id).build()).join();
            } catch (CompletionException e) {
                LOG.warnf(e.getCause(), "Failed to delete S3 data for %s", id);
            }
        }
        return removed != null;
    }

    @Override
    public boolean acquireLock(String id) {
        return locks.add(id);
    }

    @Override
    public void releaseLock(String id) {
        locks.remove(id);
    }

    @Override
    public List<String> cleanupExpiredUploads() {
        Instant now = Instant.now();
        List<String> cleaned = new ArrayList<>();
        for (Map.Entry<String, UploadInfo> entry : records.entrySet()) {
            Instant expiresAt = entry.getValue().getExpiresAt();
            if (expiresAt != null && now.isAfter(expiresAt) && acquireLock(entry.getKey())) {
                try {
                    if (discardUpload(entry.getKey())) {
                        cleaned.add(entry.getKey());
                    }
                } finally {
                    releaseLock(entry.getKey());
                }
            }
        }
        return cleaned;
    }

    // ---- test hooks ----

    /** The bytes S3 holds for a complete upload; for an incomplete one, only what is still in the tail. */
    public Optional<byte[]> readBytes(String id) {
        Multipart mp = multiparts.get(id);
        if (mp == null) {
            return Optional.empty();
        }
        if (mp.complete) {
            try {
                return Optional.of(s3.getObject(GetObjectRequest.builder().bucket(bucket).key(id).build(),
                        AsyncResponseTransformer.toBytes()).join().asByteArray());
            } catch (CompletionException e) {
                if (e.getCause() instanceof NoSuchKeyException) {
                    return Optional.empty();
                }
                throw e;
            }
        }
        if (!mp.committedParts.isEmpty()) {
            throw new AssertionError("uploaded parts of an incomplete multipart upload cannot be read back");
        }
        return Optional.of(mp.committedTail);
    }

    public byte[] objectContent(String id) {
        return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(id).build(),
                AsyncResponseTransformer.toBytes()).join().asByteArray();
    }

}
