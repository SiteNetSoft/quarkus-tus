package org.sitenetsoft.quarkus.tus.runtime.store;

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
import java.util.stream.Collectors;

@ApplicationScoped
public class LocalFileUploadStore implements UploadStore {

    private static final Logger LOG = Logger.getLogger(LocalFileUploadStore.class);

    private static final String META_SUFFIX = ".meta";
    private static final String META_TMP_SUFFIX = ".meta.tmp";

    private final Map<String, UploadInfo> uploads = new ConcurrentHashMap<>();
    private final Map<String, Long> activeLocks = new ConcurrentHashMap<>();
    private final AtomicBoolean initValidated = new AtomicBoolean(false);

    private Path uploadBaseDir;
    private long lockTimeoutMs;

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

    @Override
    public Optional<UploadInfo> findUploadInfo(String id) {
        return Optional.ofNullable(uploads.get(id));
    }

    @Override
    public String createUpload(UploadInfo info) {
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
    }

    @Override
    public void updateUploadInfo(String id, UploadInfo info) {
        if (uploads.containsKey(id)) {
            uploads.put(id, info);
            persistMetadata(id, info);
        }
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
                                // The lock spans the whole transfer; a slow client must not
                                // look abandoned while its bytes are still arriving.
                                touchLock(id);
                                return asyncFile.write(io.vertx.mutiny.core.buffer.Buffer.newInstance(buf))
                                        .replaceWith((long) buf.length());
                            })
                            .collect().with(Collectors.summingLong(Long::longValue))
                            .eventually(asyncFile::close);
                })
                .onFailure().call(e -> {
                    // Only our own file I/O is an error of ours; anything else came down the
                    // stream (a limit crossed, the client gone) and is the caller's business.
                    if (e instanceof FileSystemException || e instanceof IOException) {
                        LOG.errorf(e, "Error staging chunk for upload %s at %d — truncating to safe offset", id, offset);
                    } else {
                        LOG.debugf("Staging of upload %s at %d stopped: %s", id, offset, e.getMessage());
                    }
                    return abortChunk(id, offset)
                            .onFailure().recoverWithNull();
                });
    }

    /** Refreshes the lock's timestamp so activity, not acquisition time, decides staleness. */
    private void touchLock(String id) {
        activeLocks.computeIfPresent(id, (k, v) -> System.currentTimeMillis());
    }

    @Override
    public Uni<Void> commitChunk(String id, long offset, long bytesStaged) {
        UploadInfo info = uploads.get(id);
        if (info == null) {
            return Uni.createFrom().failure(new UploadNotFoundException(id));
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
                    return abortChunk(finalId, 0).onFailure().recoverWithNull();
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
    public boolean discardUpload(String id) {
        return discardLockedUpload(id);
    }

    /** For the store's own maintenance: takes the lock, discards, releases; false if in use. */
    private boolean discardIfUnlocked(String id) {
        if (!acquireLock(id)) {
            return false;
        }
        try {
            return discardLockedUpload(id);
        } finally {
            releaseLock(id);
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
    public boolean acquireLock(String id) {
        long now = System.currentTimeMillis();
        Long existing = activeLocks.putIfAbsent(id, now);
        if (existing == null) {
            return true;
        }
        // Check if existing lock has timed out
        if (now - existing > lockTimeoutMs) {
            if (activeLocks.replace(id, existing, now)) {
                LOG.warnf("Reclaimed stale lock for upload %s (held for %d ms)", id, now - existing);
                return true;
            }
        }
        return false;
    }

    @Override
    public void releaseLock(String id) {
        activeLocks.remove(id);
    }

    @Override
    public void cleanupStaleLocks() {
        long now = System.currentTimeMillis();
        activeLocks.entrySet().removeIf(entry -> {
            boolean stale = now - entry.getValue() > lockTimeoutMs;
            if (stale) {
                LOG.warnf("Removing stale lock for upload %s (held for %d ms)", entry.getKey(), now - entry.getValue());
            }
            return stale;
        });
    }


    private void truncateToOffset(Path file, long safeOffset) {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.truncate(safeOffset);
        } catch (IOException truncErr) {
            LOG.warnf(truncErr, "Failed to truncate file %s to offset %d", file, safeOffset);
        }
    }

    @Override
    public List<String> cleanupExpiredUploads() {
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
    public List<String> cleanupStaleUploads(long staleHours) {
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
    public int cleanupOrphanFiles() {
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
