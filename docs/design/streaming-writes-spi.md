# Streaming writes: a proposed shape for `UploadStore`

Status: **implemented (2026-08-16)** on the `streaming-spi` branch, targeting 1.0.0. The shape below
is what shipped, with these resolutions of the open questions at the bottom:

1. `commitChunk` is `Uni<Void>`.
2. Zero-length bodies never reach the store; the framework short-circuits them.
3. `validateOffset` is gone — the framework compares against `findUploadInfo().getOffset()` under
   the lock and the store re-asserts inside `stageChunk`.
4. The pending-final record is a plain `UploadInfo` (`isFinalConcat=true`, `partialIds`) that the
   framework builds and the store persists through `createUpload(UploadInfo)`;
   `concatenate(finalId, sourceIds)` fills it in later. Finished and unfinished concatenation share
   one path, and the store returns nothing — the framework already has the id.

Two further changes fell out of "the framework owns the protocol": `createUpload` takes a complete
`UploadInfo` (so `createUploadDeferred`, `setUploaderId`, `setDeferredLength`, `checkServerSizeConstraint`,
`isExpired`, `getExpiresAt` all left the SPI in favour of `updateUploadInfo`), and completion became
a *transition* — the commit that reaches `Upload-Length` — instead of a persisted latch, so
`UploadInfo.completionFired` is gone and a zero-length upload completes on creation. The TCK lives in
the `tck` module (`org.sitenetsoft:quarkus-tus-tck`).

**What the review then found (2026-08-16).** Streaming moved *when* things happen, and machinery
written for the buffered world assumed the old timing:

- The lock now spans the client's transfer, not a local disk write, so the local store's 30 s
  stale-lock reclamation fired on any chunk that took longer than that. The lock is refreshed per
  buffer while staging, the timeout is `quarkus.tus.lock-timeout-seconds`, and the SPI states that a
  lock must be treated as live while bytes flow.
- Staged bytes reach the data file before they are verified, so a crash mid-chunk left the file
  longer than the persisted offset — and restart "trusted the file size", adopting unverified bytes.
  Restart now truncates the file to the persisted offset (only committed offsets are ever persisted).
- Quarkus REST *cancels* the response pipeline when the client disconnects, and cancellation skips
  every failure handler: the store's promised `abortChunk`, the events after a commit, completion.
  The write pipeline is detached from the response (`TusUploadResource.detached`), so a disconnect
  surfaces as the body stream's own failure and takes the ordinary abort → release path.
- After a stale-offset failure nothing was staged, so the framework no longer calls
  `abortChunk(callerOffset)` — that would tell the store to roll back below the real offset.
- A store that throws from `stageChunk` instead of failing the `Uni` (the docs' own S3 sketch did)
  no longer leaks the creation-with-upload lock: the call is `deferred`, the same failure mapping
  serves POST and PATCH, and a failed creation-with-upload discards its upload. The TCK now rejects
  synchronous throws and has a failing-stream case; `BufferingUploadStore` runs `appendBytes` on a
  worker. `TusFaultyStoreTest` is the misbehaving-store harness for all of this.
- The chunk-size and entity-length limits are decided by the framework from what `ChunkStream`
  counted, not from the store handing `ChunkLimitExceededException` back in its original type — a
  store (or its SDK) that wraps stream failures used to turn 413/409 into 500.
- `discardUpload` changed contract: the framework holds the lock when it calls it (`DELETE` takes
  the lock; a finished concatenation discards its partials under the locks it already holds, closing
  the gap in which a second final could slip in), and the store no longer takes or checks it. Every
  discard path in the resource goes through one helper that also clears the progress entry.
- Over HTTP/2 a length-less creation-with-upload body is just DATA frames, so it is recognised by
  its content type rather than a `Transfer-Encoding` header — and that test surfaced that Vert.x's
  `AsyncFile` is bound to the context that opened it, which the HTTP/2 stream context is not; the
  local store now hops each write onto the file's context.
- The progress stream synthesises the event from the offset when the in-memory entry is gone (a
  restart), and a zero-length PATCH reports where the upload stands.

## The problem is not really `byte[]`

The roadmap records this item as "`writeChunkAsync` takes `byte[]`, not a stream". That is true and it
does block a real S3 backend — the AWS async SDK wants a publisher it can drain to the network as
bytes arrive, so a `byte[]` parameter forces receive-then-send instead of pipelining, and pins the
whole chunk in heap for the duration.

But reading the current implementations shows a larger problem. Here is everything
`LocalFileUploadStore.writeChunkAsync` does (`store/LocalFileUploadStore.java:682`):

1. Re-asserts the offset and raises `OffsetMismatchException` — the TOCTOU guard.
2. Validates the checksum and raises `ChecksumMismatchException`.
3. Writes the bytes.
4. Advances the offset and stamps `lastActivity`.
5. Persists the metadata sidecar.
6. Latches completion via `markCompletionFired()`, calls `uploadProgressService.finishUpload`, and
   **fires `TusUploadCompletedEvent`**.
7. Truncates the file back to the last good offset if anything failed.

Exactly one of those — number 3 — is storage. The rest is protocol.

The evidence that this is a real burden rather than a stylistic complaint is `InMemoryUploadStore`, the
test-only store in `integration-tests`. It is the simplest implementation anyone could write, and it
still has to inject `UploadProgressService` and `Event<TusUploadCompletedEvent>`, call
`markCompletionFired()`, and fire the completion event (`:323`, `:378`–`:380`). A third-party author
writing an S3 backend has to know to do all of that, from Javadoc that does not say so. Anything they
miss fails silently: no completion event means SSE never reports `complete` and observers never run.

It also explains a bug already recorded in this project: the store fires `TusUploadCompletedEvent`
*earlier in the write pipeline* than the resource fires `TusChunkReceivedEvent`, so anything observing
both sees completion before the final chunk. `TusSseEventBridge` works around that. The ordering is
inverted precisely because event firing lives in the storage layer instead of the layer that sequences
the request.

So the redesign should do both things at once. Changing `byte[]` to a stream while leaving
responsibilities 1, 2, 4, 5, 6 and 7 in the store would spend the one breaking change we get and leave
the SPI nearly as hard to implement.

## Principle

**The store owns bytes. The framework owns the protocol.**

A store should be implementable by someone who knows their storage system and has never read the TUS
spec.

## Proposed shape

Replace `writeChunkAsync` with a staged write — three flat methods rather than a `ChunkWriter` handle
(**decided 2026-08-09**). The handle would make misuse harder, but the flat form is markedly easier to
implement, and ease of implementation is the whole point of this exercise. The TCK carries the burden
of catching misuse instead.

```java
/**
 * Streams a chunk into storage at {@code offset}.
 * <p>
 * The bytes must NOT become visible and the upload's offset must NOT advance until
 * {@link #commitChunk} is called. Returning normally means the bytes are durable enough
 * to be committed, not that they are part of the upload.
 *
 * @param expectedLength the declared chunk length, for backends that need it up front
 *                       (S3 requires a content length per part); -1 if unknown
 * @return the number of bytes actually staged
 */
Uni<Long> stageChunk(String id, long offset, Multi<Buffer> data, long expectedLength);

/** Makes a staged chunk part of the upload and advances the offset to {@code offset + bytesStaged}. */
Uni<Void> commitChunk(String id, long offset, long bytesStaged);

/** Discards a staged chunk. Afterwards the upload's offset must equal {@code offset}. */
Uni<Void> abortChunk(String id, long offset);
```

`Multi<Buffer>` rather than `InputStream`: this runs on the event loop, and `InputStream` is blocking.
`expectedLength` is separate because a publisher does not carry a length and S3 needs one per part.

How the two backends satisfy it:

| | `stageChunk` | `commitChunk` | `abortChunk` |
|---|---|---|---|
| Local file | write at `offset` | set offset, persist `.meta` | `truncateToOffset(file, offset)` — already exists |
| S3 | upload a multipart part | record the ETag | abort the part |

The local store already has the abort primitive; `truncateToOffset` is used today for write failures
(`:755`). It just becomes explicit and named.

## The checksum contract

This is the part that makes streaming hard, and the reason the split above pays for itself.

TUS requires that a bad `Upload-Checksum` produce **460 with the upload's offset unchanged** — the
bytes must not be persisted. Today that is trivial: the store holds the whole array and hashes it
before writing anything (`digest.digest(data)`, `:772`). Streaming inverts the order: the digest is
only known once the last byte has already reached storage.

With a staged write, the framework can own this end to end and no store ever sees a checksum:

```
framework: wrap the incoming Multi<Buffer> in a digesting, counting operator
framework: stageChunk(id, offset, wrapped, contentLength)
framework: compare the computed digest with Upload-Checksum
   match    -> commitChunk(id, offset, n)   -> 204, offset advances
   mismatch -> abortChunk(id, offset)       -> 460, offset unchanged
   I/O error-> abortChunk(id, offset)       -> 5xx, offset unchanged
```

`ChecksumInfo` disappears from the SPI entirely. Every backend gets correct checksum behaviour, for
free, including algorithms added later. Compare with today, where each store reimplements
`validateChecksum` — `InMemoryUploadStore` has its own copy, and its own `ChecksumMismatchException`
throw sites, which is duplicated logic that can drift.

## What moves out of the store

| Responsibility | Today | Proposed |
|---|---|---|
| Offset TOCTOU re-assert | store | **both** — framework validates, store keeps the last-line check |
| Checksum validation | store | framework |
| Write bytes | store | store |
| Advance offset, stamp activity | store | store, in `commitChunk` |
| Persist metadata | store | store, in `commitChunk` |
| Progress bookkeeping | store | framework |
| Fire `TusUploadCompletedEvent` | store | framework |
| Rollback on failure | store, implicit | store, explicit as `abortChunk` |

Moving event firing to the framework also lets the completion event be sequenced *after* the chunk
event, which removes the reason `TusSseEventBridge` has to skip the completing chunk.

## Migration

The resource must change too. `TusUploadResource` declares `byte[] body` as a JAX-RS parameter
(`:210`, `:475`), so RESTEasy Reactive materialises the entire body before the method is entered —
changing only the SPI would fix nothing.

For existing implementors, ship an adapter so the upgrade is not a rewrite:

```java
/** Collects the stream and calls a byte[] method. Simple, correct, and buffers — which is the point. */
public abstract class BufferingUploadStore implements UploadStore {
    protected abstract long writeChunk(String id, long offset, byte[] chunk);
    // stageChunk collects the Multi into a byte[] and stages it; commit/abort as usual.
}
```

A store that does not care about memory extends that and keeps its current logic. A store that does
implements the three methods directly.

`writeInitialData` (`:65`, used by creation-with-upload) gets the same treatment and should become
part of the same staged flow rather than a separate synchronous path.

## What the TCK then asserts

The contract test (roadmap item 2) becomes writable, because the guarantees are now stateable:

- `stageChunk` does not advance the offset; a `findUploadInfo` between stage and commit shows the old one.
- `abortChunk` after a partial stage leaves the offset exactly where it was.
- `commitChunk` advances the offset by exactly the staged byte count.
- `stageChunk` at a stale offset raises `OffsetMismatchException`.
- Abort is idempotent, and safe to call without a preceding successful stage.
- A failed stage leaves no bytes visible.

## Concatenation, in the same pass

**Decided 2026-08-09: yes.** It is the same breaking change, and deferring it means breaking
implementors twice.

Four SPI methods handle it today — `mergePartialUploadsWithOwnership` (`:314`),
`mergePartialUploadsUnfinished` (`:434`), `isConcatReady` (`:518`), `finalizeConcatenation` (`:527`) —
and they have the same disease. `doMergeWithOwnership` validates that every source exists, is marked
partial and is complete; checks ownership against `requiredOwnerId`; applies
`checkServerSizeConstraint`; generates the final id; **joins the bytes**; builds the whole `UploadInfo`
including expiry and the verbatim `Upload-Concat` value; discards the partials; and returns a URL.
Once again exactly one step is storage.

Two further defects belong to this pass:

**Failures collapse.** There are nine `return Optional.empty()` sites in that one method, covering
source-not-found, not-partial, not-complete, ownership-denied, size-exceeded, lock contention,
missing file and `IOException`. The resource funnels all of them into a single
`mergeFailureResponse()`. A client that referenced someone else's partial and a client that hit a
full disk get the same answer, because `Optional` destroyed the distinction before the resource could
map it.

**Stores build URLs.** Four sites return `tusBuildTimeConfig.path() + "/" + id` (`:232`, `:280`,
`:420`, `:497`) — `createUpload` and `createUploadDeferred` as well as both merges. That is the
"stores return Location paths, not IDs" item on the roadmap. A store has no business knowing the HTTP
mount point, and it is the same breaking change, so it lands here.

### Proposed

One storage primitive replaces all four methods:

```java
/**
 * Creates an upload whose content is {@code sourceIds} joined in order.
 * <p>
 * The caller has already verified that every source exists, is partial, is complete and is
 * owned by the requester, and that the total is within the server limit. The store joins
 * bytes and registers the result; it validates no protocol rules.
 *
 * @return the new upload's id — an id, never a Location
 */
Uni<String> concatenate(ConcatRequest request);
```

`ConcatRequest` carries `sourceIds` plus the values the framework computed: metadata, the verbatim
`Upload-Concat` header, owner, expiry. The other three methods become framework logic:

| Today | Proposed |
|---|---|
| `mergePartialUploadsWithOwnership` | framework validates, then one `concatenate` |
| `mergePartialUploadsUnfinished` | framework records a pending final; no store call yet |
| `isConcatReady` | framework, from `findUploadInfo` on each source |
| `finalizeConcatenation` | framework calls the same `concatenate` once sources complete |

**Moving validation up solves the failure-collapsing problem by construction.** Once the framework
performs every check, it knows exactly which one failed and maps the status itself — 403 for
ownership, 409 for an incomplete source, 413 for size. The store can then only fail for I/O reasons,
so a single error channel is finally honest.

Returning `Uni` also lets the join be non-blocking, which it is not today (`Files.copy` in
`Files.newOutputStream`, `:384`–`:395`), and lets S3 use a server-side multipart copy instead of
pulling every byte through the application.

## Open questions

1. **Must `commitChunk` be async?** Local can do it synchronously. S3's complete-multipart is a network
   call. `Uni<Void>` is the safe choice but adds ceremony for simple stores.
2. **Zero-length chunks.** A zero-length PATCH completes a zero-length upload today. Does that stage
   and commit, or short-circuit?
3. **Does the framework still need `validateOffset`** as a separate SPI method once staging asserts it?
4. **Where does the pending-final record live** for unfinished concatenation? The framework needs to
   persist "final F awaits partials A, B, C" across a restart, and today that state is inside the
   store's `UploadInfo`. Either the store keeps it (and the SPI grows a way to express it) or the
   framework gains its own small persistent record. This is the one part of the concatenation split
   that is not obviously clean.
