package org.sitenetsoft.quarkus.tus.runtime.spi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;

import java.util.List;
import java.util.Optional;

/**
 * Storage backend for TUS uploads.
 * <p>
 * <strong>The store owns bytes; the framework owns the protocol.</strong> An implementation
 * persists {@link UploadInfo} records it is handed, moves bytes into place, and answers simple
 * questions about what it holds. It never validates a TUS rule, never computes a checksum,
 * never fires an event and never builds a URL — the extension does all of that before and after
 * calling into the store. A store should be implementable by someone who knows their storage
 * system and has never read the TUS specification.
 * <p>
 * Consumers provide an implementation as a CDI bean, typically
 * {@code @ApplicationScoped @Alternative @Priority(1)}. Stores that would rather receive a whole
 * chunk as a {@code byte[]} than a stream can extend {@link BufferingUploadStore} instead of
 * implementing the staged write themselves.
 *
 * <h2>Writes are staged</h2>
 * A chunk is written in two steps so that the framework can reject it after the bytes have
 * already reached storage — which is what a streaming {@code Upload-Checksum} check needs, since
 * the digest is only known once the last byte has been read:
 * <ol>
 *   <li>{@link #stageChunk} streams the bytes into place at {@code offset} but must
 *       <em>not</em> advance the upload's offset;</li>
 *   <li>{@link #commitChunk} makes them part of the upload and advances the offset, or
 *       {@link #abortChunk} discards them and leaves the upload exactly as it was.</li>
 * </ol>
 * The framework holds the upload's lock ({@link #acquireLock}) around the whole sequence. It
 * never stages a chunk it knows to be empty; a length-less body that turns out to be empty is
 * staged as zero bytes and then aborted rather than committed.
 *
 * <h2>Threading</h2>
 * Every method returns a {@code Uni} (or takes a {@code Multi}) and may be subscribed to on a
 * Vert.x event loop, so none of them may block: use the backend's asynchronous client, or
 * offload blocking work to a worker pool ({@code vertx.executeBlocking},
 * {@code Uni.runSubscriptionOn(Infrastructure.getDefaultWorkerPool())}). A store whose records
 * live in a remote service (a database, Redis, DynamoDB) needs no local index — each record
 * method can be a round trip — but a store that can answer from memory should, since the
 * record methods are called several times per request.
 * <p>
 * A reusable contract test, {@code AbstractUploadStoreContractTest} in the
 * {@code org.sitenetsoft:quarkus-tus-tck} artifact, checks an implementation against every rule
 * below.
 */
public interface UploadStore {

    /**
     * Resolves to the record for {@code id}, or empty if no such upload exists (or it was
     * discarded).
     */
    Uni<Optional<UploadInfo>> findUploadInfo(String id);

    /**
     * Persists a new upload record and prepares whatever storage it needs (an empty file, a
     * multipart upload, a row).
     * <p>
     * The framework builds the record completely — length, offset {@code 0}, metadata, partial
     * flag, deferred-length flag, expiry, uploader, {@code lastActivity} — and for a pending
     * concatenation also {@code isFinalConcat=true} with {@code partialIds}. The store's only
     * job is to allocate an id, persist the record so that {@link #findUploadInfo} returns it,
     * and return the id.
     *
     * @return the new upload's id — an opaque id, never a URL or path
     * @throws UploadStoreException (as a failure) if the record or its storage could not be created
     */
    Uni<String> createUpload(UploadInfo info);

    /**
     * Replaces the persisted record for {@code id}. The framework calls this after changing a
     * protocol attribute of the record it obtained from {@link #findUploadInfo} — the uploader
     * being set after creation, a deferred length becoming known. It is not used to advance the
     * offset; {@link #commitChunk} does that.
     * <p>
     * A no-op if the upload does not exist.
     */
    Uni<Void> updateUploadInfo(String id, UploadInfo info);

    /**
     * Streams a chunk into storage at {@code offset}.
     * <p>
     * The bytes must not become part of the upload and the offset must not advance until
     * {@link #commitChunk} is called: a {@link #findUploadInfo} between stage and commit shows
     * the old offset. Returning normally means the bytes are held safely enough to be committed.
     * If staging fails part-way, the store must leave nothing visible — the framework will call
     * {@link #abortChunk}, but a store should be safe even if it does not.
     * <p>
     * {@code offset} must be re-checked against the record: a stale offset means a request raced
     * past the framework's own validation, and writing there would overwrite bytes that were
     * already acknowledged.
     *
     * Failures come down {@code data} too — the client hung up, or the framework cut the body
     * off at a limit. Let them fail the returned {@code Uni} as they are, unwrapped: the
     * framework decides the response from the failure's type. Every failure, including a
     * missing upload or a stale offset, must be a failure of the returned {@code Uni}, never a
     * synchronous throw.
     *
     * @param data           the chunk, as a backpressured stream of buffers; subscribe exactly once
     * @param expectedLength the declared chunk length in bytes, for backends that must know it up
     *                       front (S3 needs a content length per part); {@code -1} if unknown
     * @return the number of bytes actually staged
     * @throws UploadNotFoundException (as a failure) if {@code id} does not exist
     * @throws OffsetMismatchException (as a failure) if {@code offset} is not the upload's current offset
     */
    Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength);

    /**
     * Makes the chunk staged at {@code offset} part of the upload: the offset becomes
     * {@code offset + bytesStaged}, {@code lastActivity} is stamped, and the record is persisted.
     * The store must not decide anything else — whether the upload is now complete is the
     * framework's business.
     */
    Uni<Void> commitChunk(String id, long offset, long bytesStaged);

    /**
     * Discards whatever was staged at {@code offset}. Afterwards the upload's offset must equal
     * {@code offset} and no staged byte may be visible. Idempotent, and safe to call when nothing
     * was staged or when staging itself failed.
     */
    Uni<Void> abortChunk(String id, long offset);

    /**
     * Fills the final upload {@code finalId} with the bytes of {@code sourceIds}, joined in
     * order, and marks it complete.
     * <p>
     * {@code finalId} was already created through {@link #createUpload} with
     * {@code isFinalConcat=true}; the framework has verified that every source exists, is
     * complete, and is owned by the requester, and that the total is within limits, and it
     * holds the locks. On success the store sets the final record's offset to its entity
     * length, clears {@code isFinalConcat} and {@code partialIds}, and persists it. The
     * sources are left in place — the framework discards them afterwards.
     * <p>
     * A backend that can join server-side (S3 multipart copy) should; the local store pipes
     * the files.
     *
     * @throws UploadStoreException (as a failure) if the join failed; the final upload should
     *                              then still be safe to retry
     */
    Uni<Void> concatenate(String finalId, List<String> sourceIds);

    /**
     * Deletes an upload's bytes and record. The framework holds the upload's lock when it calls
     * this — DELETE takes it, and a finished concatenation discards its partials under the locks
     * it already holds — so the store neither takes nor checks the lock here; whether anyone
     * else may be writing has already been settled by the caller. A store's own cleanup
     * ({@link #cleanupExpiredUploads} and the hooks below) must take the lock itself before
     * deleting.
     *
     * @return true if an upload was removed; false if it did not exist
     */
    Uni<Boolean> discardUpload(String id);

    /**
     * Takes the upload's exclusive lock, which the framework holds across a chunk write and a
     * concatenation. Not reentrant. A store that runs on more than one node needs a shared
     * lock here; the bundled store's lock is per process.
     * <p>
     * The lock is held for the whole of {@link #stageChunk} — that is, for as long as the
     * client takes to send the chunk. A store that expires abandoned locks must therefore treat
     * a lock as live while bytes are flowing (the bundled store refreshes it on every buffer;
     * a lease-based lock would extend the lease), and its timeout must exceed the longest pause
     * a healthy client may make mid-chunk.
     *
     * @return false if the lock is held by someone else
     */
    Uni<Boolean> acquireLock(String id);

    Uni<Void> releaseLock(String id);

    /**
     * Removes every upload whose {@link UploadInfo#getExpiresAt()} is in the past, skipping
     * those currently locked.
     *
     * @return the ids actually removed
     */
    Uni<List<String>> cleanupExpiredUploads();

    /**
     * Releases locks whose holder died. Called by the scheduler roughly once a minute.
     * <p>
     * This and the two hooks below default to doing nothing so that a store with no such concept
     * need not implement them — but a store that does hold locks or write files must, or its
     * maintenance simply never runs.
     */
    default Uni<Void> cleanupStaleLocks() {
        return Uni.createFrom().voidItem();
    }

    /**
     * Removes incomplete uploads with no activity for {@code staleHours}.
     *
     * @return the uploads actually removed, which is what the scheduler logs
     */
    default Uni<List<String>> cleanupStaleUploads(long staleHours) {
        return Uni.createFrom().item(List.of());
    }

    /**
     * Removes stored data with no corresponding upload, e.g. left by a crash.
     *
     * @return how many were removed
     */
    default Uni<Integer> cleanupOrphanFiles() {
        return Uni.createFrom().item(0);
    }
}
