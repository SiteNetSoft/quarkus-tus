package org.sitenetsoft.quarkus.tus.runtime.store;

import io.quarkus.arc.DefaultBean;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.file.FileSystemException;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStoreException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The bundled store: uploads as files in a directory, records as JSON sidecars next to them.
 * <p>
 * A {@link DefaultBean}, so that an application's own {@link UploadStore} bean replaces it by
 * merely existing — a plain {@code @ApplicationScoped} implementation is enough, no
 * {@code @Alternative} and {@code @Priority} needed (though they still work).
 */
@ApplicationScoped
@DefaultBean
public class LocalFileUploadStore implements UploadStore {

    private static final Logger LOG = Logger.getLogger(LocalFileUploadStore.class);

    private static final String META_SUFFIX = ".meta";
    private static final String META_TMP_SUFFIX = ".meta.tmp";

    private final Map<String, UploadInfo> uploads = new ConcurrentHashMap<>();
    private final Map<String, Lock> activeLocks = new ConcurrentHashMap<>();
    private final AtomicLong lockGenerations = new AtomicLong();
    private final AtomicBoolean initValidated = new AtomicBoolean(false);

    private Path uploadBaseDir;
    /** Zero disables reclamation: a lock is then held until released. */
    private long lockTimeoutMs;

    /**
     * One acquisition of an upload's lock. Reclaiming a stale lock creates a new generation
     * rather than refreshing the old one, so that the holder it was taken from — which may
     * still be streaming, its socket merely stalled — can be told apart from the new holder:
     * a stage remembers the generation it started under and refuses to write, commit or roll
     * anything back once that generation is no longer the one in the map.
     * <p>
     * The SPI carries no token, so a {@code releaseLock} cannot say whose it is. Every holder
     * still owes exactly one release, and a reclaimed lock has two holders who each owe one:
     * {@code owedReleases} counts them, and the lock only goes when the count reaches zero,
     * which stops the displaced holder's release from freeing the lock underneath the new one.
     * A holder that never releases leaves the entry idle, and {@link #cleanupStaleLocks} clears
     * it after the timeout as it would any other abandoned lock.
     */
    private static final class Lock {
        final long generation;
        volatile long lastActivity;
        final AtomicInteger owedReleases;
        /** Stages running under this generation; a commit or abort cannot be theirs while one is. */
        final AtomicInteger stagesInFlight = new AtomicInteger();

        Lock(long generation, long now, int owedReleases) {
            this.generation = generation;
            this.lastActivity = now;
            this.owedReleases = new AtomicInteger(owedReleases);
        }
    }

    @Inject
    Vertx vertx;

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @PostConstruct
    void init() {
        this.uploadBaseDir = Path.of(tusRuntimeConfig.store().local().uploadDir()).normalize();
        this.lockTimeoutMs = TimeUnit.SECONDS.toMillis(tusRuntimeConfig.lockTimeoutSeconds());
        try {
            Files.createDirectories(uploadBaseDir);
            LOG.infof("TUS uploads dir: %s", uploadBaseDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create uploads directory " + uploadBaseDir, e);
        }

        if (!Files.isWritable(uploadBaseDir)) {
            throw new RuntimeException("TUS uploads directory is not writable: " + uploadBaseDir);
        }

        if (tusRuntimeConfig.lockTimeoutSeconds() < 0) {
            throw new RuntimeException("quarkus.tus.lock-timeout-seconds must not be negative (0 disables reclamation): "
                    + tusRuntimeConfig.lockTimeoutSeconds());
        }
        if (tusRuntimeConfig.expirationHours() < 0) {
            throw new RuntimeException("quarkus.tus.expiration-hours must not be negative (0 disables expiry): "
                    + tusRuntimeConfig.expirationHours());
        }

        if (tusRuntimeConfig.maxChunkSize() > tusRuntimeConfig.maxSize()) {
            throw new RuntimeException("quarkus.tus.max-chunk-size (" + tusRuntimeConfig.maxChunkSize()
                    + ") must not exceed quarkus.tus.max-size (" + tusRuntimeConfig.maxSize() + ")");
        }

        String[] algorithms = tusRuntimeConfig.checksumAlgorithms().split(",");
        Set<String> supported = Set.of("sha1", "md5", "sha256");
        for (String alg : algorithms) {
            String trimmed = alg.trim().toLowerCase();
            if (!trimmed.isEmpty() && !supported.contains(trimmed)) {
                LOG.warnf("Unsupported checksum algorithm configured: '%s' (supported: %s)", trimmed, supported);
            }
        }

        initValidated.set(true);

        reloadPersistedUploads();
    }

    private void persistMetadata(String id, UploadInfo info) {
        try {
            Path tmpFile = uploadBaseDir.resolve(id + META_TMP_SUFFIX);
            Path metaFile = uploadBaseDir.resolve(id + META_SUFFIX);
            Files.writeString(tmpFile, info.toJson(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile, metaFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to persist metadata for upload %s", id);
        }
    }

    private void deleteMetadata(String id) {
        try {
            Files.deleteIfExists(uploadBaseDir.resolve(id + META_SUFFIX));
            Files.deleteIfExists(uploadBaseDir.resolve(id + META_TMP_SUFFIX));
        } catch (IOException e) {
            LOG.warnf(e, "Failed to delete metadata for upload %s", id);
        }
    }

    /**
     * Rescans the upload directory and reloads every persisted record. Runs at startup; a record
     * whose data file disagrees with the persisted offset is reconciled here. Public so that an
     * operator (or a test) can re-run it; it never touches an upload that is already loaded and
     * unchanged on disk beyond re-reading its record.
     */
    public void reloadPersistedUploads() {
        int loaded = 0, expired = 0, skipped = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadBaseDir, "*" + META_SUFFIX)) {
            for (Path metaFile : stream) {
                String fileName = metaFile.getFileName().toString();
                String id = fileName.substring(0, fileName.length() - META_SUFFIX.length());

                if (!org.sitenetsoft.quarkus.tus.runtime.TusUtils.isValidUuid(id)) {
                    skipped++;
                    continue;
                }

                Path dataFile = safePath(id);
                if (!Files.exists(dataFile)) {
                    LOG.warnf("Orphaned metadata file (no data file): %s — removing", fileName);
                    deleteMetadata(id);
                    skipped++;
                    continue;
                }

                try {
                    String json = Files.readString(metaFile);
                    UploadInfo info = UploadInfo.fromJson(json);

                    // Remove expired uploads
                    if (info.getExpiresAt() != null && Instant.now().isAfter(info.getExpiresAt())) {
                        Files.deleteIfExists(dataFile);
                        deleteMetadata(id);
                        expired++;
                        continue;
                    }

                    // Reconcile the data file with the persisted offset. Only committed offsets
                    // are ever persisted, while staged bytes reach the file before they are
                    // verified — so a file longer than the record holds an unverified tail from
                    // a crash mid-write, and it is cut off. A shorter file has really lost data;
                    // the record can only follow it.
                    long fileSize = Files.size(dataFile);
                    if (!info.isFinalConcat()) {
                        if (fileSize > info.getOffset()) {
                            LOG.warnf("Upload %s: data file (%d) is longer than persisted offset %d"
                                    + " — truncating unverified tail", id, fileSize, info.getOffset());
                            truncateToOffset(dataFile, info.getOffset());
                        } else if (fileSize < info.getOffset()) {
                            LOG.warnf("Upload %s: data file (%d) is shorter than persisted offset %d"
                                    + " — trusting file size", id, fileSize, info.getOffset());
                            info.setOffset(fileSize);
                        }
                    }

                    uploads.put(id, info);
                    loaded++;
                } catch (Exception e) {
                    LOG.warnf(e, "Corrupt metadata file %s — skipping", fileName);
                    skipped++;
                }
            }
        } catch (IOException e) {
            LOG.warnf(e, "Failed to scan for persisted uploads in %s", uploadBaseDir);
        }

        // Clean up orphaned .meta.tmp files
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadBaseDir, "*" + META_TMP_SUFFIX)) {
            for (Path tmpFile : stream) {
                Files.deleteIfExists(tmpFile);
            }
        } catch (IOException e) {
            LOG.warnf(e, "Failed to clean up tmp metadata files");
        }

        if (loaded > 0 || expired > 0 || skipped > 0) {
            LOG.infof("Reloaded persisted uploads: loaded=%d, expired=%d, skipped=%d", loaded, expired, skipped);
        }
    }

    private Path safePath(String id) {
        Path resolved = uploadBaseDir.resolve(id).normalize();
        if (!resolved.startsWith(uploadBaseDir)) {
            throw new SecurityException("Path traversal attempt detected for id: " + id);
        }
        return resolved;
    }

    // Records live in memory, so reads and locks answer on the calling thread; anything that
    // touches a file or a sidecar runs on a worker, never on the event loop that subscribed.

    @Override
    public Uni<Optional<UploadInfo>> findUploadInfo(String id) {
        return Uni.createFrom().item(Optional.ofNullable(uploads.get(id)));
    }

    @Override
    public Uni<String> createUpload(UploadInfo info) {
        return blocking(() -> {
            String id = UUID.randomUUID().toString();
            Path file = safePath(id);
            try {
                if (!Files.exists(file)) {
                    Files.createFile(file);
                }
            } catch (IOException e) {
                throw new UploadStoreException("Failed to create upload file for " + id, e);
            }
            uploads.put(id, info);
            persistMetadata(id, info);
            return id;
        });
    }

    @Override
    public Uni<Void> updateUploadInfo(String id, UploadInfo info) {
        if (!uploads.containsKey(id)) {
            return Uni.createFrom().voidItem();
        }
        uploads.put(id, info);
        return blocking(() -> {
            persistMetadata(id, info);
            return null;
        });
    }

    /** Runs {@code work} on a Vert.x worker; a thrown exception fails the Uni. */
    private <T> Uni<T> blocking(java.util.concurrent.Callable<T> work) {
        return vertx.executeBlocking(Uni.createFrom().item(() -> {
            try {
                return work.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new UploadStoreException(e.getMessage(), e);
            }
        }), false);
    }

    @Override
    public Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(id));
        }

        // The caller's offset is never trusted: writing at a stale one would overwrite bytes
        // that were already stored and acknowledged. Callers holding the upload's lock have
        // already validated this, so a mismatch here means the write raced past validation.
        if (offset != info.getOffset()) {
            return Uni.createFrom().failure(new OffsetMismatchException(
                    "Write at offset " + offset + " but upload " + id
                            + " is at offset " + info.getOffset(),
                    info.getOffset()));
        }

        Path file = safePath(id);
        OpenOptions openOptions = new OpenOptions().setWrite(true).setCreate(false);

        // The generation this stage runs under. Null when the caller holds no lock at all,
        // which the framework never does; then there is nothing to fence against.
        Lock owner = activeLocks.get(id);
        if (owner != null) {
            owner.stagesInFlight.incrementAndGet();
        }

        // An AsyncFile may only be used on the context that opened it, and the body's buffers
        // do not necessarily arrive there — over HTTP/2 they come in on the stream's context,
        // and a worker thread may have opened the file — so every write hops onto the file's
        // context first.
        io.vertx.mutiny.core.Context fileContext = vertx.getOrCreateContext();
        return vertx.fileSystem()
                .open(file.toString(), openOptions)
                .flatMap(asyncFile -> {
                    asyncFile.setWritePos(offset);
                    return data
                            .emitOn(command -> fileContext.runOnContext(command))
                            .onItem().transformToUniAndConcatenate(buf -> {
                                // A holder whose lock was reclaimed while it stalled must not
                                // write another byte: someone else owns this offset now.
                                if (isDisplaced(id, owner)) {
                                    return Uni.createFrom().failure(displaced(id, owner));
                                }
                                // The lock spans the whole transfer; a slow client must not
                                // look abandoned while its bytes are still arriving.
                                touchLock(id, owner);
                                return asyncFile.write(io.vertx.mutiny.core.buffer.Buffer.newInstance(buf))
                                        .replaceWith((long) buf.length());
                            })
                            .collect().with(Collectors.summingLong(Long::longValue))
                            .eventually(asyncFile::close);
                })
                // A stage that ended after its lock was reclaimed staged nothing anyone may
                // commit: its bytes are interleaved with the new holder's.
                .chain(staged -> isDisplaced(id, owner)
                        ? Uni.createFrom().failure(displaced(id, owner))
                        : Uni.createFrom().item(staged))
                .eventually(() -> {
                    if (owner != null) {
                        owner.stagesInFlight.decrementAndGet();
                    }
                })
                .onFailure().call(e -> {
                    // Only our own file I/O is an error of ours; anything else came down the
                    // stream (a limit crossed, the client gone) and is the caller's business.
                    if (e instanceof FileSystemException || e instanceof IOException) {
                        LOG.errorf(e, "Error staging chunk for upload %s at %d — truncating to safe offset", id, offset);
                    } else {
                        LOG.debugf("Staging of upload %s at %d stopped: %s", id, offset, e.getMessage());
                    }
                    if (isDisplaced(id, owner)) {
                        // The file is the new holder's now; whatever this stage left in it is
                        // theirs to overwrite, and truncating would cut their bytes.
                        return Uni.createFrom().voidItem();
                    }
                    return truncateStaged(id, offset)
                            .onFailure().recoverWithNull();
                });
    }

    /** True when {@code owner} was a lock that is no longer the upload's current one. */
    private boolean isDisplaced(String id, Lock owner) {
        return owner != null && activeLocks.get(id) != owner;
    }

    private OffsetMismatchException displaced(String id, Lock owner) {
        UploadInfo info = uploads.get(id);
        long current = info == null ? -1 : info.getOffset();
        LOG.warnf("Upload %s: lock generation %d was reclaimed while its holder was still writing"
                + " — its chunk is discarded", id, owner.generation);
        return new OffsetMismatchException("Lock on upload " + id
                + " was reclaimed while the chunk was in flight", current);
    }

    /** Refreshes the lock's timestamp so activity, not acquisition time, decides staleness. */
    private void touchLock(String id, Lock owner) {
        Lock current = activeLocks.get(id);
        if (current != null && (owner == null || current == owner)) {
            current.lastActivity = System.currentTimeMillis();
        }
    }

    @Override
    public Uni<Void> commitChunk(String id, long offset, long bytesStaged) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(id));
        }
        Throwable superseded = superseded(id, info, offset, "commit");
        if (superseded != null) {
            return Uni.createFrom().failure(superseded);
        }
        // Metadata persistence is blocking file I/O; keep it off the event loop.
        return vertx.executeBlocking(Uni.createFrom().item(() -> {
            // If the upload was discarded while this write was in flight, persisting
            // would recreate a .meta for an upload whose data file is gone.
            if (uploads.get(id) != info) {
                LOG.warnf("Upload %s was discarded during a write; discarding its result", id);
                return null;
            }
            info.setOffset(offset + bytesStaged);
            info.setLastActivity(Instant.now());
            persistMetadata(id, info);
            return null;
        }), false).replaceWithVoid();
    }

    @Override
    public Uni<Void> abortChunk(String id, long offset) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            // Nothing to roll back to; the discard took the file with it.
            return Uni.createFrom().voidItem();
        }
        Throwable superseded = superseded(id, info, offset, "abort");
        if (superseded != null) {
            return Uni.createFrom().failure(superseded);
        }
        return truncateStaged(id, offset);
    }

    /**
     * Whether a commit or abort "at {@code offset}" can still be about the chunk its caller
     * staged. The SPI carries no token, so the caller's identity is inferred from what it says
     * about the upload: the committed offset is the truth, and a caller who has it wrong staged
     * under a lock that was since reclaimed and committed past. Likewise, while a stage is
     * still running under the current lock the call cannot be that holder's — it does not
     * commit or abort until its stage has ended — so it is the displaced holder's.
     *
     * @return the failure to answer with, or null if the call is genuine
     */
    private Throwable superseded(String id, UploadInfo info, long offset, String what) {
        if (offset != info.getOffset()) {
            LOG.warnf("Upload %s: %s at offset %d refused, the upload is at %d — the caller's lock was"
                    + " reclaimed and its chunk superseded", id, what, offset, info.getOffset());
            return new OffsetMismatchException("Cannot " + what + " at offset " + offset + ": upload "
                    + id + " is at offset " + info.getOffset(), info.getOffset());
        }
        Lock current = activeLocks.get(id);
        if (current != null && current.stagesInFlight.get() > 0) {
            LOG.warnf("Upload %s: %s at offset %d refused, a chunk is still being staged under the"
                    + " current lock — the caller's lock was reclaimed", id, what, offset);
            return new OffsetMismatchException("Cannot " + what + " upload " + id
                    + ": a chunk is being staged under a newer lock", info.getOffset());
        }
        return null;
    }

    /** Cuts the file back to {@code offset}, dropping whatever a stage left past it. */
    private Uni<Void> truncateStaged(String id, long offset) {
        return vertx.executeBlocking(Uni.createFrom().item(() -> {
            Path file = safePath(id);
            if (Files.exists(file)) {
                truncateToOffset(file, offset);
            }
            return null;
        }), false).replaceWithVoid();
    }

    @Override
    public Uni<Void> concatenate(String finalId, List<String> sourceIds) {
        UploadInfo finalInfo = uploads.get(finalId);
        if (finalInfo == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(finalId));
        }
        Path finalFile = safePath(finalId);
        List<Path> sources = new ArrayList<>();
        for (String sourceId : sourceIds) {
            Path source = safePath(sourceId);
            if (!uploads.containsKey(sourceId) || !Files.exists(source)) {
                return Uni.createFrom().failure(new UploadNotFoundException(sourceId));
            }
            sources.add(source);
        }

        OpenOptions writeOptions = new OpenOptions().setWrite(true).setCreate(true).setTruncateExisting(true);
        OpenOptions readOptions = new OpenOptions().setRead(true).setWrite(false).setCreate(false);

        return vertx.fileSystem().open(finalFile.toString(), writeOptions)
                .flatMap(out -> Multi.createFrom().iterable(sources)
                        .onItem().transformToUniAndConcatenate(source ->
                                vertx.fileSystem().open(source.toString(), readOptions)
                                        .flatMap(in -> in.pipe().endOnComplete(false).endOnFailure(false).to(out)
                                                .eventually(in::close)))
                        .collect().last()
                        .eventually(out::close))
                .flatMap(v -> vertx.executeBlocking(Uni.createFrom().item(() -> {
                    finalInfo.setOffset(finalInfo.getEntityLength());
                    finalInfo.setFinalConcat(false);
                    finalInfo.setPartialIds(null);
                    finalInfo.setLastActivity(Instant.now());
                    persistMetadata(finalId, finalInfo);
                    return null;
                }), false))
                .replaceWithVoid()
                .onFailure().call(e -> {
                    LOG.errorf(e, "Failed to concatenate into %s — truncating partial merge", finalId);
                    return truncateStaged(finalId, 0).onFailure().recoverWithNull();
                })
                .onFailure().transform(e -> e instanceof UploadStoreException ? e
                        : new UploadStoreException("Failed to concatenate into " + finalId, e));
    }

    /**
     * Deletes an upload's bytes and record. The framework holds the upload's lock when it calls
     * this, so there is nothing to check here; the store's own cleanup jobs go through
     * {@link #discardIfUnlocked} instead, which takes the lock first so that they never delete
     * underneath an in-flight write.
     */
    @Override
    public Uni<Boolean> discardUpload(String id) {
        return blocking(() -> discardLockedUpload(id));
    }

    /** For the store's own maintenance: takes the lock, discards, releases; false if in use. */
    private boolean discardIfUnlocked(String id) {
        if (!tryLock(id)) {
            return false;
        }
        try {
            return discardLockedUpload(id);
        } finally {
            release(id);
        }
    }

    private boolean discardLockedUpload(String id) {
        UploadInfo removed = uploads.remove(id);

        Path file = safePath(id);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to delete upload file for %s", id);
        }
        deleteMetadata(id);

        return removed != null;
    }

    @Override
    public Uni<Boolean> acquireLock(String id) {
        return Uni.createFrom().item(tryLock(id));
    }

    private boolean tryLock(String id) {
        long now = System.currentTimeMillis();
        Lock existing = activeLocks.putIfAbsent(id, new Lock(lockGenerations.incrementAndGet(), now, 1));
        if (existing == null) {
            return true;
        }
        if (isStale(existing, now)) {
            // A new generation, not a refreshed timestamp: the old holder may still be alive
            // and must find out. It still owes its release, hence the carried-over count.
            Lock reclaimed = new Lock(lockGenerations.incrementAndGet(), now, existing.owedReleases.get() + 1);
            if (activeLocks.replace(id, existing, reclaimed)) {
                LOG.warnf("Reclaimed stale lock for upload %s (idle for %d ms)", id, now - existing.lastActivity);
                return true;
            }
        }
        return false;
    }

    private boolean isStale(Lock lock, long now) {
        return lockTimeoutMs > 0 && now - lock.lastActivity > lockTimeoutMs;
    }

    @Override
    public Uni<Void> releaseLock(String id) {
        release(id);
        return Uni.createFrom().voidItem();
    }

    /** One holder's release; the lock goes when every holder that owes one has paid. */
    private void release(String id) {
        activeLocks.computeIfPresent(id, (k, lock) -> lock.owedReleases.decrementAndGet() <= 0 ? null : lock);
    }

    /**
     * Drops idle locks nobody will release. A stale lock with a stage still in flight is left
     * alone: its holder is stalled, not dead, and will release once its stream ends — removing
     * the entry now would let that release free whichever lock a later request holds by then.
     * {@link #tryLock} reclaims such a lock on demand instead, with the release accounted for.
     */
    @Override
    public Uni<Void> cleanupStaleLocks() {
        long now = System.currentTimeMillis();
        activeLocks.entrySet().removeIf(entry -> {
            boolean stale = isStale(entry.getValue(), now) && entry.getValue().stagesInFlight.get() == 0;
            if (stale) {
                LOG.warnf("Removing stale lock for upload %s (idle for %d ms)", entry.getKey(),
                        now - entry.getValue().lastActivity);
            }
            return stale;
        });
        return Uni.createFrom().voidItem();
    }


    private void truncateToOffset(Path file, long safeOffset) {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.truncate(safeOffset);
        } catch (IOException truncErr) {
            LOG.warnf(truncErr, "Failed to truncate file %s to offset %d", file, safeOffset);
        }
    }

    @Override
    public Uni<List<String>> cleanupExpiredUploads() {
        return blocking(this::cleanupExpiredUploadsNow);
    }

    private List<String> cleanupExpiredUploadsNow() {
        List<String> expiredIds = new ArrayList<>();
        Instant now = Instant.now();

        for (Map.Entry<String, UploadInfo> entry : uploads.entrySet()) {
            UploadInfo info = entry.getValue();
            Instant expiresAt = info.getExpiresAt();
            if (expiresAt != null && now.isAfter(expiresAt)) {
                expiredIds.add(entry.getKey());
            }
        }

        // An upload being written is skipped rather than deleted underneath the write, and is
        // retried on the next run. Only what was actually removed is reported as cleaned.
        List<String> cleanedIds = new ArrayList<>();
        for (String id : expiredIds) {
            if (discardIfUnlocked(id)) {
                cleanedIds.add(id);
            } else {
                LOG.infof("Skipped expired upload %s: in use, will retry next run", id);
            }
        }

        if (!cleanedIds.isEmpty()) {
            LOG.infof("Cleaned up %d expired uploads: %s", cleanedIds.size(), String.join(", ", cleanedIds));
        }

        return cleanedIds;
    }

    /**
     * Removes incomplete uploads that have had no activity for the given number of hours.
     */
    @Override
    public Uni<List<String>> cleanupStaleUploads(long staleHours) {
        return blocking(() -> cleanupStaleUploadsNow(staleHours));
    }

    private List<String> cleanupStaleUploadsNow(long staleHours) {
        if (staleHours <= 0) {
            return List.of();
        }

        Instant cutoff = Instant.now().minus(staleHours, ChronoUnit.HOURS);
        List<String> staleIds = new ArrayList<>();

        for (Map.Entry<String, UploadInfo> entry : uploads.entrySet()) {
            UploadInfo info = entry.getValue();
            // Only clean up incomplete uploads
            if (info.getOffset() >= info.getEntityLength() && info.getEntityLength() >= 0) {
                continue;
            }
            Instant lastActivity = info.getLastActivity();
            if (lastActivity != null && lastActivity.isBefore(cutoff)) {
                staleIds.add(entry.getKey());
            }
        }

        List<String> cleanedIds = new ArrayList<>();
        for (String id : staleIds) {
            LOG.infof("Cleaning up stale upload %s (no activity since %s)", id,
                    uploads.get(id) != null ? uploads.get(id).getLastActivity() : "unknown");
            if (discardIfUnlocked(id)) {
                cleanedIds.add(id);
            } else {
                LOG.infof("Skipped stale upload %s: in use, will retry next run", id);
            }
        }

        if (!cleanedIds.isEmpty()) {
            LOG.infof("Cleaned up %d stale uploads", cleanedIds.size());
        }

        return cleanedIds;
    }

    /**
     * Scans the upload directory for data files with no matching in-memory entry
     * and no .meta sidecar file. These are orphans from crashes or incomplete cleanup.
     */
    @Override
    public Uni<Integer> cleanupOrphanFiles() {
        return blocking(this::cleanupOrphanFilesNow);
    }

    private int cleanupOrphanFilesNow() {
        int cleaned = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadBaseDir)) {
            for (Path file : stream) {
                String name = file.getFileName().toString();
                // Skip metadata files
                if (name.endsWith(META_SUFFIX) || name.endsWith(META_TMP_SUFFIX)) {
                    continue;
                }
                // Only consider UUID-named files
                if (!org.sitenetsoft.quarkus.tus.runtime.TusUtils.isValidUuid(name)) {
                    continue;
                }
                // If there's no in-memory entry and no .meta file, it's an orphan
                if (!uploads.containsKey(name) && !Files.exists(uploadBaseDir.resolve(name + META_SUFFIX))) {
                    LOG.infof("Removing orphan data file: %s", name);
                    Files.deleteIfExists(file);
                    cleaned++;
                }
            }
        } catch (IOException e) {
            LOG.warnf(e, "Failed to scan for orphan files in %s", uploadBaseDir);
        }
        if (cleaned > 0) {
            LOG.infof("Cleaned up %d orphan files", cleaned);
        }
        return cleaned;
    }
}
