# Streaming Writes SPI Implementation Plan

**Goal:** Replace the `byte[]`-based `UploadStore` SPI with a staged streaming write (`stageChunk`/`commitChunk`/`abortChunk`), move every protocol responsibility (checksum, completion events, progress, validation, Location building, concatenation rules) out of the store into the resource layer, collapse concatenation to one `concatenate` primitive, and ship a reusable contract test (TCK) that both bundled stores pass.

**Architecture:** The resource no longer declares a `byte[]` body parameter; Quarkus REST leaves the request paused, and the resource drains it as `Multi<Buffer>` (via `HttpServerRequest.toMulti()`, `fetch()`-based backpressure) through a digesting/counting operator into `stageChunk`. The framework then decides commit vs abort (checksum, limits) and fires events. Stores only move bytes and persist the `UploadInfo` record the framework hands them. `BufferingUploadStore` adapts the streaming contract back to a `byte[]` append for simple stores (the in-memory test store uses it). The TCK lives in the runtime module's test fixtures.

**Tech Stack:** Quarkus 3.38 (Quarkus REST / RESTEasy Reactive, Mutiny, Vert.x 4 mutiny bindings), JUnit 5, Gradle `java-test-fixtures`.

**Spec:** `docs/design/streaming-writes-spi.md` (decisions there are settled; this plan resolves its four open questions — see Global Constraints).

## Global Constraints

- Java 25, `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`; run tests with `./gradlew :integration-tests:test` (231 tests green on master before this work).
- No storage-vendor SDKs in `runtime`/`deployment`. Runtime deps stay: quarkus-arc, quarkus-rest, quarkus-scheduler, quarkus-vertx.
- Dependency versions come from the Quarkus BOM — never pin.
- Decided in the spec (do not relitigate): flat `stageChunk`/`commitChunk`/`abortChunk`, not a handle; concatenation fixed in the same pass; stores never return Locations.
- Open questions resolved here: (1) `commitChunk` is `Uni<Void>`; (2) zero-length bodies never reach the store — the framework short-circuits; (3) `validateOffset` is removed, the framework compares against `findUploadInfo().getOffset()` under the lock and the store re-asserts inside `stageChunk`; (4) the pending-final record is a plain `UploadInfo` (`isFinalConcat=true`, `partialIds`) that the framework builds and the store persists through `createUpload(UploadInfo)` — `concatenate(finalId, sourceIds)` fills it in later, so finished and unfinished concatenation share one path.
- Completion is decided by **transition** (a commit that moves offset from `< entityLength` to `== entityLength`, or creation/deferred-length-set of a zero-length upload), not by a persisted latch. `UploadInfo.completionFired` is removed.
- Public SPI types use core `io.vertx.core.buffer.Buffer`, `io.smallrye.mutiny.Uni/Multi`.

---

### Task 1: New SPI surface (compiles alone; nothing implements it yet)

**Files:**
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/spi/UploadStore.java` (rewrite)
- Create: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/spi/UploadNotFoundException.java`
- Create: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/spi/UploadStoreException.java`
- Delete: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/spi/ChecksumMismatchException.java`
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/model/UploadInfo.java` — remove `completionFired`, `markCompletionFired()`, `isCompletionFired`, `setCompletionFired`, JSON key; remove `ChecksumInfo` nested class → move to `runtime/.../TusUtils.ChecksumInfo`? No: create `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/ChecksumInfo.java` (record `ChecksumInfo(String algorithm, String value)`) and update `TusUtils.parseChecksumHeader`.

**Interfaces (Produces):**

```java
package org.sitenetsoft.quarkus.tus.runtime.spi;
public interface UploadStore {
    Optional<UploadInfo> findUploadInfo(String id);
    /** Persist a record the framework built; allocate and return the id. Throws UploadStoreException. */
    String createUpload(UploadInfo info);
    /** Replace the persisted record for id (framework changed uploaderId / entityLength / ...). */
    void updateUploadInfo(String id, UploadInfo info);
    Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength);
    Uni<Void> commitChunk(String id, long offset, long bytesStaged);
    Uni<Void> abortChunk(String id, long offset);
    /** finalId already exists (created via createUpload with isFinalConcat=true). Join sourceIds' bytes into it,
        then set offset=entityLength, finalConcat=false, partialIds=null and persist. Do NOT delete sources. */
    Uni<Void> concatenate(String finalId, List<String> sourceIds);
    boolean discardUpload(String id);
    boolean acquireLock(String id);
    void releaseLock(String id);
    List<String> cleanupExpiredUploads();
    default void cleanupStaleLocks() {}
    default List<String> cleanupStaleUploads(long staleHours) { return List.of(); }
    default int cleanupOrphanFiles() { return 0; }
}
```
`UploadNotFoundException(String id)` and `UploadStoreException(String, Throwable)` are `RuntimeException`s. `OffsetMismatchException` unchanged.

- [ ] Write the interface with full Javadoc per method (contract: stage must not advance offset; commit advances by exactly `bytesStaged` and stamps `lastActivity`; abort idempotent and safe without prior stage; stage at stale offset → `OffsetMismatchException`; unknown id → `UploadNotFoundException`; `expectedLength == -1` when unknown).
- [ ] Update `UploadInfo` and `TusUtils.parseChecksumHeader` (returns `Optional<ChecksumInfo>`).
- [ ] `./gradlew :runtime:compileJava` — expected to FAIL (stores/resource still reference old methods). That is fine; Task 2–4 fix it. Do not commit yet.

### Task 2: `BufferingUploadStore` adapter + `InMemoryUploadStore` on top of it

**Files:**
- Create: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/spi/BufferingUploadStore.java`
- Rewrite: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/InMemoryUploadStore.java`

**Produces:**
```java
public abstract class BufferingUploadStore implements UploadStore {
    /** Append data at offset; store bytes only. Offset/lastActivity are advanced by the adapter via updateUploadInfo. */
    protected abstract void appendBytes(String id, long offset, byte[] data);
    // stageChunk: findUploadInfo → UploadNotFound; offset != info.offset → OffsetMismatch;
    //   collect Multi into byte[] → staged.put(id, bytes) → item(bytes.length)
    // commitChunk: bytes = staged.remove(id) (null && bytesStaged==0 → ok); appendBytes; info.setOffset(offset+bytesStaged);
    //   info.setLastActivity(now); updateUploadInfo(id, info); item(null)
    // abortChunk: staged.remove(id); voidItem()
}
```
`InMemoryUploadStore` keeps `hasData/getData`, `Map<String,UploadInfo>`, `Map<String,byte[]>`, lock set; implements `createUpload(UploadInfo)` (UUID id, put, `data.put(id, new byte[0])`), `updateUploadInfo`, `appendBytes`, `concatenate` (join arrays, update final info), `discardUpload`, locks, `cleanupExpiredUploads`. **No** injection of `UploadProgressService`, `Event`, `TusBuildTimeConfig`, `TusRuntimeConfig`.

- [ ] Write both. Compile still fails until Task 3/4.

### Task 3: `LocalFileUploadStore` on the new SPI

**Files:**
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/store/LocalFileUploadStore.java`

- [ ] Remove injections of `UploadProgressService`, `Event<TusUploadCompletedEvent>`, `TusBuildTimeConfig`. Keep `Vertx`, `TusRuntimeConfig` (upload dir, config validation).
- [ ] `createUpload(UploadInfo)`: UUID, `uploads.put`, create empty file, `persistMetadata`; on IOException remove and throw `UploadStoreException`.
- [ ] `updateUploadInfo(id, info)`: `uploads.put(id, info); persistMetadata`.
- [ ] `stageChunk`: not found → `UploadNotFoundException`; offset mismatch → `OffsetMismatchException`; open `AsyncFile` (write, create=false), `setWritePos(offset)`, `data.onItem().transformToUniAndConcatenate(buf -> file.write(Buffer.newInstance(buf)).replaceWith((long) buf.length())).collect().with(Collectors.summingLong(...))`, `.eventually(file::close)`; on failure `truncateToOffset(file, offset)`.
- [ ] `commitChunk`: `info.setOffset(offset+n)`, `lastActivity`, persist via `vertx.executeBlocking`; guard `uploads.get(id) == info` (discarded mid-write → skip persist, as today).
- [ ] `abortChunk`: `truncateToOffset(safePath(id), offset)` (ignore missing file); voidItem.
- [ ] `concatenate(finalId, sourceIds)`: async: open final for write (truncate), for each source in order open read → `pipeTo` final (sequential via `transformToUniAndConcatenate`), close; then update final info (offset=len, finalConcat=false, partialIds=null, lastActivity) + persist on worker; on failure truncate final to 0 and fail with `UploadStoreException`.
- [ ] Delete: `createUploadDeferred`, `setUploaderId`, `getUploaderId`, `setDeferredLength`, `hasDeferredLength`, `mergePartialUploads*`, `doMergeWithOwnership`, `concatValueFor`, `isConcatReady`, `finalizeConcatenation`, `checkServerSizeConstraint`, `validateOffset`, `writeChunkAsync`, `validateChecksum`, `writeInitialData`, `isExpired`, `getExpiresAt`. `discardLockedUpload` no longer calls progress service.
- [ ] `./gradlew :runtime:compileJava` — still fails on resource/authorizer only.

### Task 4: Resource + framework logic

**Files:**
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/TusUploadResource.java` (POST, PATCH, HEAD auto-finalize, DELETE progress)
- Create: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/ChunkStream.java` — helper that turns `RoutingContext` into `Multi<Buffer>` with `Expect: 100-continue` handling, byte counting, optional `MessageDigest`, and limit enforcement (fails with `ChunkLimitExceededException` if count > maxChunkSize or offset+count > entityLength).
- Create: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/UploadRecords.java` — static builders: `newUpload(length, metadata, partial, deferred, uploader, expiryHours)`, `newFinalConcat(total, metadata, uploader, ids, header, expiryHours)`.
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/TusUploadAuthorizer.java:40` — `uploadStore.findUploadInfo(id).map(UploadInfo::getUploaderId).orElse(null)`.
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/UploadExpirationScheduler.java` — call `uploadProgressService.finishUpload(id)` for each cleaned id.
- Modify: `runtime/src/main/java/org/sitenetsoft/quarkus/tus/runtime/sse/TusSseEventBridge.java` — completion is now fired *after* the chunk event by the framework; drop the "skip completing chunk" workaround if the SSE tests still pass with plain ordering (verify; keep otherwise).

Behaviour to implement (PATCH):
1. Same pre-checks as today (412/400/404/410/403). Expiry: `info.getExpiresAt() != null && now.isAfter(...)` → discard → 410.
2. `Content-Length` header > maxChunkSize → 413. Parse `Upload-Checksum` → 400s.
3. Lock → 423. Under lock: refresh; deferred length via `Upload-Length` header (≤ maxSize else 400) → `updateUploadInfo` + `progress.startUpload`; if new length is 0 → fire completion; still deferred → 400; offset mismatch → 409 with `Upload-Offset`; content-length overflow → 409.
4. Content-Length == 0 → no store call: fire `TusChunkReceivedEvent(0)`, 204 with current offset, release lock.
5. Else `ChunkStream.of(routingContext, checksumInfo, maxChunkSize, entityLength - offset)` → `store.stageChunk(id, offset, stream.multi(), contentLength or -1)` → `stream.verifyChecksum()`: mismatch → `abortChunk` → 460; ok → `commitChunk` → `.emitOn(worker)` → progress update, `TusChunkReceivedEvent`, if `offset < len && offset+n == len` → `progress.finishUpload` + `TusUploadCompletedEvent`; SSE progress; 204 `Upload-Offset: offset+n`. Failures → `abortChunk` then map: `OffsetMismatchException`→409, `ChunkLimitExceededException`→413/409, `UploadNotFoundException`→404, else 500. `.eventually(releaseLock)`.
6. Early exits with a body: `.eventually(() -> drainOrClose(routingContext, contentLength))` — resume (discard) when Content-Length ≤ maxChunkSize, else `response.endHandler(v -> response.close())`. Only on paths that did not consume the body.

POST: build record with `UploadRecords`; `store.createUpload`; `progress.startUpload` (non-deferred); fire created event; if declared length 0 → fire completion immediately; creation-with-upload: same streaming path as PATCH (offset 0, no checksum, `Content-Type` check, else discard+400). Final concat: validate all (missing/denied → 400 probe-safe; not partial / unknown length → 400; sum > maxSize → 413); `createUpload(finalRecord)`; if all complete → `finalizeConcatenation(finalId)`; 201 with `Location: TUS_PATH + "/" + id`.

`finalizeConcatenation(finalId, info)` (shared by POST and HEAD): `acquireLock(finalId)` (fail → skip/423); lock every partial (fail → release all, return "not ready"); re-check every partial exists & complete under lock; `store.concatenate(finalId, ids).await()`? — No: keep it reactive in PATCH but HEAD/POST are sync methods today. Decision: HEAD and POST return `Uni<Response>` after this task; concatenate is awaited reactively. On success release partial locks, `discardUpload` each partial, fire `TusConcatenationCompletedEvent`; on failure log, (POST) discard final → 500.

- [ ] Implement; `./gradlew :runtime:compileJava :integration-tests:compileTestJava` (fix `TusEdgeCaseTest` sites in Task 5 first if compile fails there).

### Task 5: Fix existing tests, run the suite

**Files:**
- Modify: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/TusEdgeCaseTest.java:118-140` — replace `writeChunkAsync` with `stageChunk(id, 0, Multi.createFrom().item(Buffer.buffer("BB")), 2)`; expect `OffsetMismatchException`; `:720` — replace `writeInitialData` with stage(0)+commit; keep file-size assertion.
- Modify: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/TusMetadataPersistenceTest.java:39-67` — remove the `completionFired` round-trip assertions.
- [ ] `./gradlew :integration-tests:test` — all green (expect 231 minus removed assertions). Fix regressions in Task 4 code, not by loosening tests, unless a test pinned store-owned behaviour that no longer exists (document each such change in the commit message).
- [ ] Commit: `Stream chunk bodies through a staged UploadStore SPI`.

### Task 6: TCK in runtime test fixtures + both stores pass it

**Files:**
- Modify: `runtime/build.gradle` — `id 'java-test-fixtures'`; `testFixturesImplementation enforcedPlatform(...)`, `testFixturesApi 'org.junit.jupiter:junit-jupiter-api'`, `testFixturesApi 'io.smallrye.reactive:mutiny'`, `testFixturesApi 'io.vertx:vertx-core'`.
- Create: `runtime/src/testFixtures/java/org/sitenetsoft/quarkus/tus/tck/AbstractUploadStoreContractTest.java`
- Modify: `integration-tests/build.gradle` — `testImplementation testFixtures(project(':runtime'))`.
- Create: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/LocalFileUploadStoreContractTest.java` (`@QuarkusTest`, `@Inject UploadStore`, `readBytes` reads the file).
- Create: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/InMemoryUploadStoreContractTest.java` (`@TestProfile(TusCustomStoreTest.CustomStoreProfile.class)`, `readBytes` from `getData`).

TCK contract (each a `@Test`):
1. `createUpload` returns id; `findUploadInfo` shows length and offset 0. 2. unknown id → empty. 3. `updateUploadInfo` persists uploaderId and entityLength. 4. stage does not advance offset. 5. commit advances by staged count; bytes visible via `readBytes` if provided. 6. abort after stage leaves offset unchanged; re-stage+commit at same offset works and content has no leftover. 7. stage at stale offset → `OffsetMismatchException`, offset unchanged. 8. abort without stage is a no-op. 9. stage on unknown id → `UploadNotFoundException`. 10. multi-buffer stage sums lengths. 11. zero-length stage returns 0; commit 0 leaves offset. 12. `concatenate`: final record (created with `isFinalConcat`) becomes complete; content = A+B when readable; sources still exist (framework deletes them). 13. `discardUpload` → gone; unknown → false. 14. lock semantics. 15. `cleanupExpiredUploads` removes a record with past `expiresAt`.

- [ ] Write TCK, wire Gradle, write both subclasses, `./gradlew :integration-tests:test`. Commit: `Add UploadStore contract test and run both stores through it`.

### Task 7: Docs

**Files:**
- Rewrite: `docs/modules/ROOT/pages/storage-spi.adoc` (new interface, responsibilities table from the design doc, `BufferingUploadStore`, TCK usage with the `test-fixtures` classifier, S3 sketch using stage=UploadPart / commit=record ETag / abort=drop part / concatenate=UploadPartCopy).
- Modify: `docs/modules/ROOT/pages/testing.adoc:176` — remove the "must fire TusUploadCompletedEvent" warning; describe TCK.
- Modify: `README.md:125` — streaming now supported.
- Modify: `docs/design/streaming-writes-spi.md` — status: implemented; record the four resolutions above.
- [ ] Commit: `Document the streaming UploadStore SPI and its contract test`.
