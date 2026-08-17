package org.sitenetsoft.quarkus.tus.tck;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@link UploadStore} contract as executable assertions.
 * <p>
 * Extend this class in your own test module, return your store from {@link #store()}, and every
 * rule the extension relies on is checked against it: staged writes that do not move the offset
 * until committed, aborts that leave no trace, stale-offset rejection, concatenation, locking,
 * expiry cleanup. A store that passes will behave correctly under the extension; a store that
 * fails one of these will misbehave in a way the HTTP-level tests may not show.
 * <p>
 * The store is exercised the way the extension exercises it: records are built here and handed
 * to {@link UploadStore#createUpload}, and the store's lock is held around each staged write.
 * Override {@link #readBytes} to let the content assertions run too — the SPI has no read API,
 * so without it only offsets are checked.
 * <p>
 * Typical use inside a Quarkus application:
 * <pre>{@code
 * @QuarkusTest
 * class MyStoreContractTest extends AbstractUploadStoreContractTest {
 *     @Inject UploadStore store;
 *     @Override protected UploadStore store() { return store; }
 * }
 * }</pre>
 */
public abstract class AbstractUploadStoreContractTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /** The store under test. Called once per assertion; return the same instance each time. */
    protected abstract UploadStore store();

    /**
     * The bytes currently stored for {@code id}, if the store can produce them. Default: empty,
     * which skips the content assertions.
     */
    protected Optional<byte[]> readBytes(String id) {
        return Optional.empty();
    }

    // ---- helpers ----

    protected static UploadInfo record(long entityLength) {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(entityLength);
        info.setOffset(0);
        Instant now = Instant.now();
        info.setLastActivity(now);
        info.setExpiresAt(now.plus(1, ChronoUnit.HOURS));
        return info;
    }

    protected static Multi<Buffer> bytes(String... chunks) {
        return Multi.createFrom().iterable(List.of(chunks))
                .onItem().transform(s -> Buffer.buffer(s.getBytes(StandardCharsets.UTF_8)));
    }

    protected static <T> T await(Uni<T> uni) {
        return uni.await().atMost(TIMEOUT);
    }

    /**
     * Calls {@code stageChunk} and awaits it, failing the test if the store threw synchronously
     * instead of returning a failed {@code Uni}. The framework tolerates a synchronous throw, but
     * it is not the contract: a failure must be a failure of the returned {@code Uni}.
     */
    protected long stage(String id, long offset, Multi<Buffer> data, long expectedLength) {
        Uni<Long> staged;
        try {
            staged = store().stageChunk(id, offset, data, expectedLength);
        } catch (RuntimeException e) {
            throw new AssertionError("stageChunk must return a failed Uni, not throw synchronously: " + e, e);
        }
        return await(staged);
    }

    protected String create(long entityLength) {
        return store().createUpload(record(entityLength));
    }

    protected UploadInfo info(String id) {
        return store().findUploadInfo(id).orElseThrow(() -> new AssertionError("upload " + id + " vanished"));
    }

    /** Stage + commit under the lock, as the extension does. Returns the new offset. */
    protected long write(String id, long offset, String content) {
        assertTrue(store().acquireLock(id), "lock must be free");
        try {
            long staged = stage(id, offset, bytes(content), content.length());
            await(store().commitChunk(id, offset, staged));
            return offset + staged;
        } finally {
            store().releaseLock(id);
        }
    }

    protected void assertContent(String id, String expected) {
        readBytes(id).ifPresent(actual ->
                assertEquals(expected, new String(actual, StandardCharsets.UTF_8), "stored bytes for " + id));
    }

    // ---- records ----

    @Test
    public void createUploadReturnsIdAndPersistsRecord() {
        UploadInfo record = record(42);
        record.setMetadata("filename dGVzdA==");
        record.setPartial(true);
        record.setUploaderId("alice");
        String id = store().createUpload(record);

        assertNotNull(id);
        assertFalse(id.isBlank());
        assertFalse(id.contains("/"), "createUpload must return an id, not a path or Location");
        UploadInfo found = info(id);
        assertEquals(42, found.getEntityLength());
        assertEquals(0, found.getOffset());
        assertEquals("filename dGVzdA==", found.getMetadata());
        assertTrue(found.isPartial());
        assertEquals("alice", found.getUploaderId());
        assertNotNull(found.getExpiresAt());
    }

    @Test
    public void findUploadInfoIsEmptyForUnknownId() {
        assertTrue(store().findUploadInfo(UUID.randomUUID().toString()).isEmpty());
    }

    @Test
    public void updateUploadInfoPersistsProtocolChanges() {
        UploadInfo record = record(-1);
        record.setDeferredLength(true);
        String id = store().createUpload(record);

        UploadInfo current = info(id);
        current.setUploaderId("bob");
        current.setEntityLength(500);
        current.setDeferredLength(false);
        store().updateUploadInfo(id, current);

        UploadInfo reread = info(id);
        assertEquals("bob", reread.getUploaderId());
        assertEquals(500, reread.getEntityLength());
        assertFalse(reread.isDeferredLength());
    }

    @Test
    public void updateUploadInfoOnUnknownIdIsANoOp() {
        String id = UUID.randomUUID().toString();
        store().updateUploadInfo(id, record(10));
        assertTrue(store().findUploadInfo(id).isEmpty(), "updateUploadInfo must not create records");
    }

    // ---- staged writes ----

    @Test
    public void stageDoesNotAdvanceOffsetUntilCommit() {
        String id = create(10);
        assertTrue(store().acquireLock(id));
        try {
            long staged = await(store().stageChunk(id, 0, bytes("hello"), 5));
            assertEquals(5, staged);
            assertEquals(0, info(id).getOffset(), "offset must not move on stage");

            await(store().commitChunk(id, 0, staged));
            assertEquals(5, info(id).getOffset(), "commit advances by the staged count");
            assertNotNull(info(id).getLastActivity());
        } finally {
            store().releaseLock(id);
        }
        assertContent(id, "hello");
    }

    @Test
    public void commitAdvancesByExactlyTheStagedCountAcrossChunks() {
        String id = create(11);
        assertEquals(5, write(id, 0, "hello"));
        assertEquals(6, write(id, 5, " "));
        assertEquals(11, write(id, 6, "world"));
        assertEquals(11, info(id).getOffset());
        assertContent(id, "hello world");
    }

    @Test
    public void multiBufferStageSumsTheBuffers() {
        String id = create(9);
        assertTrue(store().acquireLock(id));
        try {
            long staged = await(store().stageChunk(id, 0, bytes("abc", "def", "ghi"), 9));
            assertEquals(9, staged);
            await(store().commitChunk(id, 0, staged));
        } finally {
            store().releaseLock(id);
        }
        assertEquals(9, info(id).getOffset());
        assertContent(id, "abcdefghi");
    }

    @Test
    public void abortAfterStageLeavesOffsetAndBytesUntouched() {
        String id = create(10);
        write(id, 0, "ab");

        assertTrue(store().acquireLock(id));
        try {
            long staged = await(store().stageChunk(id, 2, bytes("XXXX"), 4));
            assertEquals(4, staged);
            await(store().abortChunk(id, 2));
            assertEquals(2, info(id).getOffset(), "abort must leave the offset where it was");
        } finally {
            store().releaseLock(id);
        }
        assertContent(id, "ab");

        // The same offset must be writable again, and nothing from the aborted stage may leak.
        write(id, 2, "cd");
        assertEquals(4, info(id).getOffset());
        assertContent(id, "abcd");
    }

    @Test
    public void abortWithoutStageIsANoOp() {
        String id = create(10);
        write(id, 0, "abc");
        await(store().abortChunk(id, 3));
        await(store().abortChunk(id, 3));
        assertEquals(3, info(id).getOffset());
        assertContent(id, "abc");
    }

    @Test
    public void stageAtStaleOffsetIsRejected() {
        String id = create(10);
        write(id, 0, "abc");

        OffsetMismatchException e = assertThrows(OffsetMismatchException.class,
                () -> stage(id, 0, bytes("ZZ"), 2));
        assertEquals(3, e.getExpectedOffset(), "the exception must carry the real offset");
        assertEquals(3, info(id).getOffset());
        assertContent(id, "abc");

        assertThrows(OffsetMismatchException.class,
                () -> stage(id, 7, bytes("ZZ"), 2), "an offset past the end is stale too");
    }

    /**
     * The body stream can fail part-way — the client hung up, or the framework cut it off at a
     * limit. The store must surface that failure as-is (the framework decides the response from
     * its type) and, once aborted, leave the upload exactly as it was.
     */
    @Test
    public void failedStreamLeavesNothingVisibleAfterAbort() {
        String id = create(10);
        write(id, 0, "abc");
        IllegalStateException boom = new IllegalStateException("stream cut");
        Multi<Buffer> failing = Multi.createBy().concatenating().streams(bytes("de"), Multi.createFrom().failure(boom));

        assertTrue(store().acquireLock(id));
        try {
            Throwable seen = assertThrows(Throwable.class, () -> stage(id, 3, failing, -1));
            assertSame(boom, seen, "the stream's failure must be propagated unwrapped");
            await(store().abortChunk(id, 3));
        } finally {
            store().releaseLock(id);
        }
        assertEquals(3, info(id).getOffset());
        assertContent(id, "abc");
        assertEquals(6, write(id, 3, "fgh"), "the upload must still accept the next chunk");
    }

    @Test
    public void stageOnUnknownUploadFails() {
        assertThrows(UploadNotFoundException.class,
                () -> stage(UUID.randomUUID().toString(), 0, bytes("x"), 1));
    }

    @Test
    public void zeroLengthStageAndCommitLeaveOffsetAlone() {
        String id = create(10);
        write(id, 0, "abc");
        assertTrue(store().acquireLock(id));
        try {
            long staged = await(store().stageChunk(id, 3, Multi.createFrom().empty(), 0));
            assertEquals(0, staged);
            await(store().commitChunk(id, 3, 0));
        } finally {
            store().releaseLock(id);
        }
        assertEquals(3, info(id).getOffset());
        assertContent(id, "abc");
    }

    @Test
    public void unknownExpectedLengthIsAccepted() {
        String id = create(10);
        assertTrue(store().acquireLock(id));
        try {
            long staged = await(store().stageChunk(id, 0, bytes("hello"), -1));
            assertEquals(5, staged);
            await(store().commitChunk(id, 0, staged));
        } finally {
            store().releaseLock(id);
        }
        assertEquals(5, info(id).getOffset());
    }

    // ---- concatenation ----

    @Test
    public void concatenateFillsTheFinalUploadInOrder() {
        String a = create(5);
        write(a, 0, "hello");
        String b = create(6);
        write(b, 0, " world");

        UploadInfo finalRecord = record(11);
        finalRecord.setFinalConcat(true);
        finalRecord.setPartialIds(List.of(a, b));
        finalRecord.setUploadConcatMergedValue("final;/tus/" + a + " /tus/" + b);
        String finalId = store().createUpload(finalRecord);
        assertEquals(0, info(finalId).getOffset());
        assertTrue(info(finalId).isFinalConcat());

        assertTrue(store().acquireLock(finalId));
        assertTrue(store().acquireLock(a));
        assertTrue(store().acquireLock(b));
        try {
            await(store().concatenate(finalId, List.of(a, b)));
        } finally {
            store().releaseLock(b);
            store().releaseLock(a);
            store().releaseLock(finalId);
        }

        UploadInfo done = info(finalId);
        assertEquals(11, done.getOffset(), "a concatenated upload is complete");
        assertEquals(11, done.getEntityLength());
        assertFalse(done.isFinalConcat());
        assertNull(done.getPartialIds());
        assertEquals("final;/tus/" + a + " /tus/" + b, done.getUploadConcatMergedValue(),
                "the verbatim Upload-Concat value must survive");
        assertContent(finalId, "hello world");

        assertTrue(store().findUploadInfo(a).isPresent(), "sources are the framework's to discard");
        assertTrue(store().findUploadInfo(b).isPresent());
    }

    @Test
    public void concatenateOnUnknownFinalFails() {
        String a = create(1);
        write(a, 0, "x");
        assertThrows(UploadNotFoundException.class,
                () -> await(store().concatenate(UUID.randomUUID().toString(), List.of(a))));
    }

    // ---- discard and locks ----

    @Test
    public void discardRemovesRecordAndBytes() {
        String id = create(3);
        write(id, 0, "abc");
        assertTrue(store().discardUpload(id));
        assertTrue(store().findUploadInfo(id).isEmpty());
        assertTrue(readBytes(id).isEmpty() || readBytes(id).get().length == 0, "bytes must be gone");
        assertFalse(store().discardUpload(id), "discarding again reports nothing removed");
        assertFalse(store().discardUpload(UUID.randomUUID().toString()));
    }

    @Test
    public void discardRefusesWhileLocked() {
        String id = create(3);
        assertTrue(store().acquireLock(id));
        try {
            assertFalse(store().discardUpload(id), "must not delete underneath a lock holder");
            assertTrue(store().findUploadInfo(id).isPresent());
        } finally {
            store().releaseLock(id);
        }
        assertTrue(store().discardUpload(id));
    }

    @Test
    public void lockIsExclusiveAndReleasable() {
        String id = create(3);
        assertTrue(store().acquireLock(id));
        assertFalse(store().acquireLock(id), "second acquisition must fail while held");
        store().releaseLock(id);
        assertTrue(store().acquireLock(id), "acquirable again after release");
        store().releaseLock(id);
        store().releaseLock(id); // releasing an unheld lock is harmless
    }

    // ---- expiry ----

    @Test
    public void cleanupExpiredUploadsRemovesOnlyExpiredRecords() {
        UploadInfo expired = record(3);
        expired.setExpiresAt(Instant.now().minus(1, ChronoUnit.HOURS));
        String expiredId = store().createUpload(expired);
        String liveId = create(3);

        List<String> cleaned = store().cleanupExpiredUploads();

        assertTrue(cleaned.contains(expiredId), "expired upload must be reported as cleaned");
        assertFalse(cleaned.contains(liveId));
        assertTrue(store().findUploadInfo(expiredId).isEmpty());
        assertTrue(store().findUploadInfo(liveId).isPresent());
    }
}
