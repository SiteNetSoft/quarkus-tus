# TUS Client Design

**Status:** approved design, pre-implementation
**Date:** 2026-08-23
**Deliverable:** a TUS 1.0.0 protocol client shipped as a Quarkus extension, published as
`org.sitenetsoft:quarkus-tus-client` (+ `-deployment`), version-locked with the server.

## Purpose

Let a dev use this project for the client half of TUS — uploading to any TUS server — with the
same stack, idiom and conformance stance as the server half. The two are independently
consumable: server only, client only, or both. The client is also the toolkit a dev needs to
build their own forwarding (`UploadStore` → another TUS server); this project deliberately ships
the primitives, never the destination (no relay store, no S3 store — see the project's
"primitives, not destinations" rule).

Non-goals: a download/GET client (TUS defines no retrieval); a JavaScript client; any relay or
piping component (a planned separate microservice consumes this extension instead).

## Module & artifact layout

Two new Gradle modules beside `runtime`/`deployment`/`tck`/`integration-tests`:

| Module | Published artifact | Contents |
|---|---|---|
| `client-runtime` | `org.sitenetsoft:quarkus-tus-client` | everything that exists at application runtime |
| `client-deployment` | `org.sitenetsoft:quarkus-tus-client-deployment` | build-time steps: bean registration, config wiring, native-image reflection |

- Package root `org.sitenetsoft.quarkus.tus.client.runtime`.
- Both ids go into `publishedArtifactIds` in the root `build.gradle`. **The artifactId trap
  applies:** the runtime jar's `META-INF/quarkus-extension.properties` will name
  `org.sitenetsoft:quarkus-tus-client-deployment`, and the published id must match it exactly —
  a mismatch breaks every consumer while in-repo tests stay green. Verify post-publish with a
  scratch consumer app, as was done for the server.
- Version is the root project's version, released in lockstep by the same tag and workflow.

### Dependency rules

- `client-runtime` depends on `quarkus-arc`, `quarkus-vertx` and Mutiny only. **No
  `quarkus-rest`** (nothing to serve), **no storage-vendor SDKs**, and — hard rule — **no
  dependency on any server module**, or "client only" would drag the server's JAX-RS resources
  into the consumer's application. The few shared shapes (metadata map encoding, checksum
  algorithm names) are duplicated deliberately; they are small and stable.
- Dependency versions come from the Quarkus BOM; nothing is pinned.

## API surface — two layers

### Low level: `TusProtocolClient`

One method per protocol operation, 1:1 with the spec, all Mutiny, **no hidden retries** — one
call is one HTTP request and the failure is the caller's. This is the relay-author's toolkit:
`offset()` for discovery, `patch()` for forwarding a chunk, `terminate()` for
delete-and-restart.

```java
Uni<TusServerCapabilities> options();                          // Tus-Version, Tus-Extension, Tus-Max-Size
Uni<TusUpload> create(TusCreateOptions opts);                  // length | deferred; metadata; partial flag;
                                                               //   optional creation-with-upload body
Uni<Long> offset(String uploadUrl);                            // HEAD — the resume primitive
Uni<Long> patch(String uploadUrl, long offset,
                Multi<Buffer> data, TusPatchOptions opts);     // resolves to the new offset
Uni<TusUpload> concatenate(List<String> partialUrls,
                           TusCreateOptions opts);             // POST Upload-Concat: final
Uni<Void> terminate(String uploadUrl);                         // DELETE
```

`TusProtocolClient` is constructible without CDI (plain constructor taking a Vert.x instance and
a config object), which is what makes fake-server unit tests cheap.

### High level: `TusClient.upload()`

One entry point: `Uni<TusUploadResult> upload(TusUploadRequest request)`. The client does
create → chunked PATCH → HEAD-and-resume on failure → done, emitting progress. Per-request
options override config.

Sources are an interface; re-readability is what makes resume, checksum and parallelism possible:

```java
interface UploadSource {
    long length();                        // -1 → creation-defer-length
    Multi<Buffer> slice(long fromOffset); // re-readable from any offset
}
```

A file implementation is bundled. A raw one-shot `Multi<Buffer>` is also accepted but honestly
degraded: no resume-from-offset, no checksum, no parallelism — requesting those with a one-shot
source is a typed error, never a silent fallback.

### Injection & instantiation

`@Inject TusClient` binds to `quarkus.tus.client.*` (the default target server).
`TusClient.create(config)` builds programmatic instances for other targets — a consumer talking
to several TUS servers makes one per target.

## Protocol scope

Full parity with the server: core + creation, creation-with-upload, creation-defer-length,
checksum, expiration, concatenation (+ concatenation-unfinished), termination — 8/9.

- **checksum-trailer is out** for the same reason as on the server's master: it needs Vert.x
  trailer support, this time *sending* request trailers from the HTTP client, which stock Vert.x
  also lacks. It joins the existing `checksum-trailer` wait; when Vert.x ships, the client side
  is a follow-up on that branch.
- **Capability check:** the high level calls `options()` once per target (cached) and refuses
  features the server does not advertise — a typed error naming the missing extension. Same
  stance as the server: never rely on what is not advertised. The low level does not check;
  it sends what it is told.
- **Checksum forces one bounded buffer.** `Upload-Checksum` is a header, so the digest must
  exist before the first body byte — the client-side mirror of why checksum-trailer exists.
  With checksum on, the chunk is read once to digest and once to send (or buffered once,
  bounded by chunk-size). Checksum off → pure streaming, never more than in-flight buffers.
- **Parallel upload is concatenation:** known-length re-readable source and `parallelism > 1` →
  split into partials, upload concurrently with bounded concurrency, `concatenate()` the final.
- **Defer-length:** create with `Upload-Defer-Length: 1`; `Upload-Length` is declared on the
  first PATCH issued *after* the length becomes known. A one-shot source only learns its length
  at end-of-stream — after that chunk's headers are gone — so the client then issues one final
  empty-bodied PATCH whose only job is the `Upload-Length` header. (The server must accept a
  zero-length PATCH that sets a deferred length; the mutual-conformance tests pin this.)
- **Expiration:** `Upload-Expires` is parsed and surfaced on `TusUpload`; resuming a known-
  expired upload fails fast with the expired-carrying not-found error rather than a doomed HEAD.

## Transport

Vert.x `HttpClient` (mutiny bindings), `Multi<Buffer>` piped into the request with backpressure.
Known chunk length → `Content-Length` per PATCH. `X-HTTP-Method-Override` is not used — real
PATCH/DELETE (the override exists for limited clients; this is not one).

## Configuration — `quarkus.tus.client.*` (runtime phase)

| Key | Default | Notes |
|---|---|---|
| `url` | — | Target TUS endpoint for the injected client. Unset boots fine; use fails fast. |
| `chunk-size` | `10485760` | Bytes per PATCH. |
| `checksum-algorithm` | *(empty = off)* | Validated at first use against the server's advertised list. |
| `max-retries` | `3` | High-level only. |
| `retry-backoff` | `1s` | Initial; exponential, capped at `30s`. |
| `parallelism` | `1` | `>1` = concatenation-based parallel upload. |
| `connect-timeout`, `request-timeout` | Vert.x defaults | |

- No build-time config: adding the dependency is the opt-in.
- **Auth is not config.** A dev-provided CDI bean implementing `TusRequestCustomizer` is applied
  to every outgoing request — tokens, tenant headers, anything — without this project inventing
  an auth model.
- These keys are client behavior, not protocol surface; the strict-conformance rule
  ("never invent protocol options") is untouched.

## Errors, retry, progress

Typed exceptions off one `TusClientException` base, mapped from status codes:
`TusOffsetMismatch` (409), `TusUploadNotFound` (404/410; carries whether expiry is the known
cause), `TusVersionMismatch` (412), `TusPayloadTooLarge` (413), `TusChecksumMismatch` (460),
`TusServerError` (5xx).

- **Low level: never retries.** Hidden retries would corrupt a relay's offset bookkeeping.
- **High level owns the resume loop.** Network failure, 5xx, 409 and 460 → HEAD resync → resume
  from the server's offset (a checksum failure re-sends the chunk — the extension working as
  designed), exponential backoff, bounded by `max-retries`. All other 4xx fail fast: they mean
  a bug, not weather.
- **Progress:** `TusUploadRequest.onProgress(Consumer<TusUploadProgress>)` — bytes sent, total,
  per-partial detail under parallelism. A callback, not a `Multi`; ignoring it costs nothing.

## Testing

- **Mutual conformance is the centerpiece.** Client integration tests live in the existing
  `integration-tests` module and drive the in-repo server over real HTTP: every supported
  extension, resume after a stream killed mid-chunk, parallel-concat reassembly verified
  byte-for-byte, good and bad checksums, defer-length, termination, expiration.
- **Error paths against a scripted fake** — a plain Vert.x `HttpServer` returning exactly the
  409/412/5xx/malformed responses a healthy server will not produce on demand. These live in
  `client-runtime`'s own test sourceset as plain JUnit (no QuarkusTest), using the CDI-free
  constructor.
- **Native:** client usage joins the existing native integration test so the reflection
  registration is actually proven.
- CI needs no new services; the MinIO job is unaffected.

## Documentation

- New `docs/modules/ROOT/pages/client.adoc`: both layers, resume semantics, the one-shot-source
  degradation table, parallel upload, `TusRequestCustomizer`, and the config reference.
- `index.adoc` gains the client artifact's coordinates and the server/client/both consumption
  matrix.

## Follow-ups (recorded, deliberately not in scope)

- Interop CI job against `tusd` — valuable once the client exists; our server is the
  conformance reference until then.
- Client-side checksum-trailer, when Vert.x ships trailer sending.
- A Dev UI card for the client — only if demand appears.
