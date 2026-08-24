# TUS Client Implementation Plan

**Goal:** A TUS 1.0.0 upload client shipped as a Quarkus extension (`org.sitenetsoft:quarkus-tus-client` + `-deployment`), with a low-level protocol client (one method per TUS operation, no hidden retries) and a high-level `upload()` that does create → chunked PATCH → HEAD-and-resume, at full 8/9 extension parity with the server.

**Architecture:** Two new Gradle modules, `client-runtime` and `client-deployment`, mirroring the server pair. `TusProtocolClient` wraps a Vert.x `HttpClient` (mutiny bindings) and is constructible without CDI; `TusClient` layers the resume loop, checksum, defer-length and concatenation-parallelism on top of it, driven by an `UploadSource` abstraction whose `slice(fromOffset)` re-readability gates resume/checksum/parallelism. CDI/config wiring lives in the deployment module. Protocol-level tests run against a scripted in-process Vert.x fake server; conformance tests drive the real in-repo server.

**Tech Stack:** Quarkus 3.38 (arc, vertx), Mutiny, smallrye-mutiny-vertx-core, JUnit 5, Gradle.

**Spec:** `docs/design/specs/2026-08-23-tus-client-design.md` — decisions there are settled; read it first.

## Global Constraints

- Java 25: `export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` before every Gradle command.
- Baseline before task 1: `./gradlew build -x nativeIntegrationTest` green on `master` — 325 tests (352 with `TUS_S3_ENDPOINT` pointing at a MinIO). Do not proceed on a red baseline.
- Work on a branch off `master` named `tus-client`.
- Dependency versions come from the Quarkus BOM (`enforcedPlatform`) — never pin a version in a module build file.
- **Client modules must never depend on server modules** (`:runtime`, `:deployment`, `:tck`). Shared shapes are duplicated deliberately.
- No `quarkus-rest` and no storage-vendor SDK anywhere in the client modules.
- Package root: `org.sitenetsoft.quarkus.tus.client.runtime` (deployment: `org.sitenetsoft.quarkus.tus.client.deployment`).
- Commit messages: imperative summary + explanatory body, no attribution or footer lines.
- Every request the client sends carries `Tus-Resumable: 1.0.0` (OPTIONS included — harmless, uniform).
- All public async methods return `Uni`/`Multi` (Mutiny), use `io.vertx.core.buffer.Buffer`.

---

### Task 1: Module scaffolding — the extension builds and its descriptor is right

**Files:**
- Modify: `settings.gradle` (add includes)
- Modify: `build.gradle` (extend `publishedArtifactIds`)
- Create: `client-runtime/build.gradle`
- Create: `client-deployment/build.gradle`
- Create: `client-deployment/src/main/java/org/sitenetsoft/quarkus/tus/client/deployment/TusClientProcessor.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/package-info.java`

**Interfaces:**
- Produces: modules `:client-runtime`, `:client-deployment`; feature name `tus-client`.

- [ ] **Step 1: settings.gradle**

```groovy
include 'runtime', 'deployment', 'tck', 'integration-tests', 'client-runtime', 'client-deployment'
```

- [ ] **Step 2: publishedArtifactIds in root build.gradle**

```groovy
ext.publishedArtifactIds = [runtime: 'quarkus-tus', deployment: 'quarkus-tus-deployment', tck: 'quarkus-tus-tck',
                            'client-runtime': 'quarkus-tus-client', 'client-deployment': 'quarkus-tus-client-deployment']
```

- [ ] **Step 3: client-runtime/build.gradle** (modelled on `runtime/build.gradle`; extension plugin must be told the deployment module since the directory is not named `deployment`)

```groovy
plugins {
    id 'io.quarkus.extension'
}

quarkusExtension {
    deploymentModule = 'client-deployment'
}

dependencies {
    implementation enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}")
    implementation 'io.quarkus:quarkus-arc'
    implementation 'io.quarkus:quarkus-vertx'
    implementation 'io.smallrye.reactive:smallrye-mutiny-vertx-core'

    annotationProcessor enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}")
    annotationProcessor 'io.quarkus:quarkus-extension-processor'
}
```

- [ ] **Step 4: client-deployment/build.gradle**

```groovy
plugins {
    id 'java-library'
}

dependencies {
    implementation enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}")
    implementation 'io.quarkus:quarkus-core-deployment'
    implementation 'io.quarkus:quarkus-arc-deployment'
    implementation 'io.quarkus:quarkus-vertx-deployment'
    implementation project(':client-runtime')

    annotationProcessor enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}")
    annotationProcessor 'io.quarkus:quarkus-extension-processor'
}
```

- [ ] **Step 5: minimal processor**

```java
package org.sitenetsoft.quarkus.tus.client.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class TusClientProcessor {

    private static final String FEATURE = "tus-client";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }
}
```

`package-info.java` in client-runtime holds only the package declaration and a one-line javadoc so the module has a compilation unit.

- [ ] **Step 6: build and verify the descriptor — this is the artifactId trap check**

Run: `./gradlew :client-runtime:build :client-deployment:build`
Then: `unzip -p client-runtime/build/libs/*.jar META-INF/quarkus-extension.properties`
Expected: `deployment-artifact=org.sitenetsoft\:quarkus-tus-client-deployment\:1.0.0`
If the group or artifact differs, fix `quarkusExtension`/`publishedArtifactIds` until it matches exactly. Do not continue with a wrong descriptor.

- [ ] **Step 7: full build stays green** — `./gradlew build -x nativeIntegrationTest`

- [ ] **Step 8: Commit** — `git add -A && git commit -m "Scaffold the client extension modules"`

---

### Task 2: Shared shapes — metadata encoding, models, exceptions, config

**Files:**
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusClientUtils.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/model/TusUpload.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/model/TusServerCapabilities.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/error/` — `TusClientException`, `TusOffsetMismatchException`, `TusUploadNotFoundException`, `TusVersionMismatchException`, `TusPayloadTooLargeException`, `TusChecksumMismatchException`, `TusServerErrorException`, `TusCapabilityException`, `TusProtocolException`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/config/TusClientRuntimeConfig.java`
- Test: `client-runtime/src/test/java/org/sitenetsoft/quarkus/tus/client/runtime/TusClientUtilsTest.java`

Add to `client-runtime/build.gradle`: `testImplementation 'io.quarkus:quarkus-junit5'` is NOT needed — these are plain JUnit tests; add `testImplementation 'org.junit.jupiter:junit-jupiter'` and `testRuntimeOnly 'org.junit.platform:junit-platform-launcher'`, plus `test { useJUnitPlatform() }`.

**Interfaces (produced, used by every later task):**

```java
public record TusUpload(String url, long offset, long length, Optional<Instant> expiresAt) {}
public record TusServerCapabilities(List<String> versions, Set<String> extensions,
                                    OptionalLong maxSize, Set<String> checksumAlgorithms) {
    public boolean supports(String extension) { return extensions.contains(extension); }
}
public class TusClientException extends RuntimeException { ... }          // base, message ctor
public class TusOffsetMismatchException extends TusClientException {}      // 409
public class TusUploadNotFoundException extends TusClientException {       // 404 / 410
    public boolean knownExpired();
}
public class TusVersionMismatchException extends TusClientException {}     // 412
public class TusPayloadTooLargeException extends TusClientException {}     // 413
public class TusChecksumMismatchException extends TusClientException {}    // 460
public class TusServerErrorException extends TusClientException {          // 5xx
    public int status();
}
public class TusCapabilityException extends TusClientException {           // feature not advertised
    public String missingExtension();
}
public class TusProtocolException extends TusClientException {}            // malformed response

public final class TusClientUtils {
    public static String encodeMetadata(Map<String, String> metadata);     // "" for empty
    public static TusClientException fromStatus(int status, boolean expired); // the mapping table
}
```

Config (`@ConfigMapping(prefix = "quarkus.tus.client")`, `@ConfigRoot(phase = ConfigPhase.RUN_TIME)`), keys per spec: `Optional<String> url()`; `@WithDefault("10485760") long chunkSize()`; `Optional<String> checksumAlgorithm()`; `@WithDefault("3") int maxRetries()`; `@WithDefault("1S") Duration retryBackoff()`; `@WithDefault("30S") Duration retryBackoffMax()`; `@WithDefault("1") int parallelism()`; `Optional<Duration> connectTimeout()`; `Optional<Duration> requestTimeout()`.

- [ ] **Step 1: failing tests** — metadata encoding per TUS spec (comma-separated `key base64value` pairs, keys never encoded, empty value = key alone) and the status table:

```java
@Test
void encodesMetadataAsBase64Pairs() {
    var encoded = TusClientUtils.encodeMetadata(new TreeMap<>(Map.of("filename", "cat.png", "flag", "")));
    assertEquals("filename Y2F0LnBuZw==,flag", encoded);
}

@Test
void emptyMetadataEncodesToEmptyString() {
    assertEquals("", TusClientUtils.encodeMetadata(Map.of()));
}

@Test
void mapsStatusesToTypedExceptions() {
    assertInstanceOf(TusOffsetMismatchException.class, TusClientUtils.fromStatus(409, false));
    assertInstanceOf(TusUploadNotFoundException.class, TusClientUtils.fromStatus(404, false));
    assertTrue(((TusUploadNotFoundException) TusClientUtils.fromStatus(410, true)).knownExpired());
    assertInstanceOf(TusVersionMismatchException.class, TusClientUtils.fromStatus(412, false));
    assertInstanceOf(TusPayloadTooLargeException.class, TusClientUtils.fromStatus(413, false));
    assertInstanceOf(TusChecksumMismatchException.class, TusClientUtils.fromStatus(460, false));
    assertEquals(503, ((TusServerErrorException) TusClientUtils.fromStatus(503, false)).status());
    assertInstanceOf(TusProtocolException.class, TusClientUtils.fromStatus(302, false));
}
```

- [ ] **Step 2: run, watch fail** — `./gradlew :client-runtime:test` — compile error (classes missing) is the expected failure.
- [ ] **Step 3: implement** — `encodeMetadata` iterates entries, `Base64.getEncoder()` on UTF-8 value bytes, joins with `,`; `fromStatus` is one switch. Exceptions are trivial subclasses; keep each in its own file.
- [ ] **Step 4: run, watch pass** — `./gradlew :client-runtime:test`; then `./gradlew build -x nativeIntegrationTest`.
- [ ] **Step 5: Commit** — `git commit -m "Add the client's shared shapes: models, errors, metadata encoding, config"`

---

### Task 3: Scripted fake server + `TusProtocolClient.options()` / `create()`

**Files:**
- Create: `client-runtime/src/test/java/org/sitenetsoft/quarkus/tus/client/runtime/ScriptedTusServer.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusProtocolClient.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusTarget.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusCreateOptions.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusRequestCustomizer.java`
- Test: `client-runtime/src/test/java/org/sitenetsoft/quarkus/tus/client/runtime/TusProtocolClientTest.java`

**Interfaces:**
- Produces:

```java
public interface TusRequestCustomizer { void customize(String method, String url, MultiMap headers); }

public final class TusTarget {                       // builder
    public static Builder builder(String url);       // .connectTimeout(d) .requestTimeout(d)
                                                     // .customizer(TusRequestCustomizer) .build()
    public String url();
}

public final class TusCreateOptions {                // builder, all optional
    public static Builder builder();                 // .length(long) | .deferLength()
                                                     // .metadata(Map<String,String>) .partial()
                                                     // .body(Multi<Buffer>, long bodyLength)  // creation-with-upload
}

public class TusProtocolClient {
    public TusProtocolClient(Vertx vertx, TusTarget target);   // io.vertx.core.Vertx
    public Uni<TusServerCapabilities> options();
    public Uni<TusUpload> create(TusCreateOptions opts);
    public void close();
}
```

- Test fixture:

```java
/** In-process Vert.x HttpServer; enqueue canned responses, capture what the client actually sent. */
final class ScriptedTusServer implements AutoCloseable {
    record Recorded(String method, String path, MultiMap headers, Buffer body) {}
    record Canned(int status, Map<String, String> headers, Buffer body) {
        static Canned of(int status, Map<String, String> headers) { return new Canned(status, headers, Buffer.buffer()); }
    }
    void enqueue(Canned response);          // FIFO; a request with an empty queue answers 599
    List<Recorded> recorded();
    String url();                            // http://localhost:<port>/tus
    // start(): vertx.createHttpServer().requestHandler(req -> req.bodyHandler(body -> { record; pop & write; }))
}
```

- [ ] **Step 1: write the fixture and a self-test** (enqueue a 204, hit it with Vert.x HttpClient, assert recorded method/headers/body). This is scaffolding folded into the task that needs it.
- [ ] **Step 2: failing tests for options() and create()**

```java
@Test
void optionsParsesCapabilities() {
    server.enqueue(Canned.of(204, Map.of(
            "Tus-Resumable", "1.0.0", "Tus-Version", "1.0.0",
            "Tus-Extension", "creation,termination,checksum", "Tus-Max-Size", "1073741824",
            "Tus-Checksum-Algorithm", "sha1,md5")));
    var caps = client.options().await().atMost(TIMEOUT);
    assertTrue(caps.supports("checksum"));
    assertEquals(OptionalLong.of(1073741824L), caps.maxSize());
    assertEquals(Set.of("sha1", "md5"), caps.checksumAlgorithms());
    var sent = server.recorded().getFirst();
    assertEquals("OPTIONS", sent.method());
    assertEquals("1.0.0", sent.headers().get("Tus-Resumable"));
}

@Test
void createSendsLengthAndMetadataAndResolvesRelativeLocation() {
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/abc123")));
    var upload = client.create(TusCreateOptions.builder().length(42)
            .metadata(Map.of("filename", "cat.png")).build()).await().atMost(TIMEOUT);
    assertEquals(server.url() + "/abc123", upload.url());       // relative Location resolved
    assertEquals(0, upload.offset());
    assertEquals(42, upload.length());
    var sent = server.recorded().getFirst();
    assertEquals("POST", sent.method());
    assertEquals("42", sent.headers().get("Upload-Length"));
    assertEquals("filename Y2F0LnBuZw==", sent.headers().get("Upload-Metadata"));
}

@Test
void createWithUploadStreamsTheBodyAndReturnsTheOffset() {
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0",
            "Location", "/tus/abc", "Upload-Offset", "5")));
    var upload = client.create(TusCreateOptions.builder().length(10)
            .body(Multi.createFrom().item(Buffer.buffer("hello")), 5).build()).await().atMost(TIMEOUT);
    assertEquals(5, upload.offset());
    var sent = server.recorded().getFirst();
    assertEquals("application/offset+octet-stream", sent.headers().get("Content-Type"));
    assertEquals("hello", sent.body().toString());
}

@Test
void deferLengthSendsTheDeferHeader() {
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/x")));
    client.create(TusCreateOptions.builder().deferLength().build()).await().atMost(TIMEOUT);
    assertEquals("1", server.recorded().getFirst().headers().get("Upload-Defer-Length"));
}

@Test
void partialCreateSendsUploadConcatPartial() {
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p1")));
    client.create(TusCreateOptions.builder().length(5).partial().build()).await().atMost(TIMEOUT);
    assertEquals("partial", server.recorded().getFirst().headers().get("Upload-Concat"));
}
```

- [ ] **Step 3: run, watch fail** — compile failure on missing classes.
- [ ] **Step 4: implement.** `TusProtocolClient` holds a `io.vertx.mutiny.core.http.HttpClient` (`new io.vertx.mutiny.core.Vertx(vertx).createHttpClient(options)`). One private `request(method, url, headers, body, expectedStatuses)` helper: builds `RequestOptions` with absolute URI, sets `Tus-Resumable`, runs the customizer, streams the body `Multi<Buffer>` if present (`req.send(Flowable/Multi)` — with mutiny bindings: `req.send(Multi<Buffer>)` overload; set `Content-Length` when the length is known, else chunked), maps non-expected statuses through `TusClientUtils.fromStatus` (410 → expired=true), missing-required-header → `TusProtocolException`. `create` resolves a relative `Location` against the target URL (`URI.create(target.url()).resolve(location)`), parses `Upload-Offset` (absent → 0) and `Upload-Expires` (RFC 7231 date → `Optional<Instant>`).
- [ ] **Step 5: run, watch pass**, full build green.
- [ ] **Step 6: Commit** — `git commit -m "Speak OPTIONS and POST: capabilities, create, creation-with-upload"`

---

### Task 4: `offset()` and `patch()`

**Files:**
- Modify: `TusProtocolClient.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusPatchOptions.java`
- Test: extend `TusProtocolClientTest.java`

**Interfaces:**
- Produces:

```java
public final class TusPatchOptions {                 // builder, all optional
    public static TusPatchOptions none();
    public static Builder builder();                 // .contentLength(long)
                                                     // .checksum(String algorithm, String base64Digest)
                                                     // .declareUploadLength(long)   // defer-length resolution
}
public Uni<Long> offset(String uploadUrl);                                        // HEAD
public Uni<Long> patch(String uploadUrl, long offset, Multi<Buffer> data, TusPatchOptions opts);
```

- [ ] **Step 1: failing tests**

```java
@Test
void offsetReadsUploadOffsetFromHead() {
    server.enqueue(Canned.of(200, Map.of("Tus-Resumable", "1.0.0",
            "Upload-Offset", "17", "Cache-Control", "no-store")));
    assertEquals(17L, client.offset(server.url() + "/abc").await().atMost(TIMEOUT));
    assertEquals("HEAD", server.recorded().getFirst().method());
}

@Test
void patchStreamsAtOffsetAndReturnsTheNewOffset() {
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "10")));
    long newOffset = client.patch(server.url() + "/abc", 5,
            Multi.createFrom().item(Buffer.buffer("hello")),
            TusPatchOptions.builder().contentLength(5).build()).await().atMost(TIMEOUT);
    assertEquals(10L, newOffset);
    var sent = server.recorded().getFirst();
    assertEquals("PATCH", sent.method());
    assertEquals("5", sent.headers().get("Upload-Offset"));
    assertEquals("application/offset+octet-stream", sent.headers().get("Content-Type"));
    assertEquals("5", sent.headers().get("Content-Length"));
    assertEquals("hello", sent.body().toString());
}

@Test
void patchCanCarryChecksumAndDeclaredLength() {
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "5")));
    client.patch(server.url() + "/abc", 0, Multi.createFrom().item(Buffer.buffer("hello")),
            TusPatchOptions.builder().contentLength(5)
                    .checksum("sha1", "qvTGHdzF6KLavt4PO0gs2a6pQ00=")
                    .declareUploadLength(5).build()).await().atMost(TIMEOUT);
    var sent = server.recorded().getFirst();
    assertEquals("sha1 qvTGHdzF6KLavt4PO0gs2a6pQ00=", sent.headers().get("Upload-Checksum"));
    assertEquals("5", sent.headers().get("Upload-Length"));
}

@Test
void patchOffsetMismatchIsTyped() {
    server.enqueue(Canned.of(409, Map.of("Tus-Resumable", "1.0.0")));
    var failure = assertThrows(TusOffsetMismatchException.class, () ->
            client.patch(server.url() + "/abc", 3, Multi.createFrom().item(Buffer.buffer("x")),
                    TusPatchOptions.none()).await().atMost(TIMEOUT));
}
```

- [ ] **Step 2: run, watch fail.** **Step 3: implement** on the shared `request` helper; a missing `Upload-Offset` on a 2xx is `TusProtocolException`. **Step 4: run, watch pass; full build.** 
- [ ] **Step 5: Commit** — `git commit -m "Speak HEAD and PATCH: offset discovery and chunk upload"`

---

### Task 5: `terminate()`, `concatenate()`, the full error table

**Files:**
- Modify: `TusProtocolClient.java`; Test: extend `TusProtocolClientTest.java`

**Interfaces:**
- Produces: `Uni<Void> terminate(String uploadUrl)`; `Uni<TusUpload> concatenate(List<String> partialUrls, TusCreateOptions opts)`.

- [ ] **Step 1: failing tests**

```java
@Test
void terminateSendsDelete() {
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0")));
    client.terminate(server.url() + "/abc").await().atMost(TIMEOUT);
    assertEquals("DELETE", server.recorded().getFirst().method());
}

@Test
void concatenateJoinsPartialUrlsSpaceSeparated() {
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/final1")));
    var fin = client.concatenate(List.of(server.url() + "/p1", server.url() + "/p2"),
            TusCreateOptions.builder().build()).await().atMost(TIMEOUT);
    var sent = server.recorded().getFirst();
    assertEquals("final;" + server.url() + "/p1 " + server.url() + "/p2",
            sent.headers().get("Upload-Concat"));
    assertNull(sent.headers().get("Upload-Length"));   // spec: final creation has no Upload-Length
}

/** Every row of the mapping table, exercised through a real HTTP exchange. */
@ParameterizedTest
@CsvSource({"404,TusUploadNotFoundException", "410,TusUploadNotFoundException",
            "412,TusVersionMismatchException", "413,TusPayloadTooLargeException",
            "460,TusChecksumMismatchException", "500,TusServerErrorException",
            "503,TusServerErrorException"})
void errorStatusesAreTyped(int status, String exceptionSimpleName) {
    server.enqueue(Canned.of(status, Map.of("Tus-Resumable", "1.0.0")));
    var failure = assertThrows(TusClientException.class, () ->
            client.offset(server.url() + "/abc").await().atMost(TIMEOUT));
    assertEquals(exceptionSimpleName, failure.getClass().getSimpleName());
}
```

- [ ] **Step 2–4: fail → implement → pass; full build.**
- [ ] **Step 5: Commit** — `git commit -m "Speak DELETE and Upload-Concat, and type every error status"`

---

### Task 6: `UploadSource` and the file implementation

**Files:**
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/source/UploadSource.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/source/FileUploadSource.java`
- Test: `client-runtime/src/test/java/org/sitenetsoft/quarkus/tus/client/runtime/source/FileUploadSourceTest.java`

**Interfaces:**
- Produces:

```java
public interface UploadSource {
    long length();                              // -1 → defer-length
    Multi<Buffer> slice(long fromOffset);       // independent, re-readable
    static UploadSource ofFile(Vertx vertx, Path path) { return new FileUploadSource(vertx, path); }
    static UploadSource oneShot(Multi<Buffer> data, long declaredLength);  // degraded; length may be -1
    default boolean replayable() { ... }        // false only for oneShot
}
```

- [ ] **Step 1: failing tests** — write a temp file with known bytes; assert `length()`; `slice(0)` yields the full content; `slice(7)` yields the suffix; two sequential `slice(0)` reads are both complete (re-readability); `oneShot` reports `replayable() == false` and a second `slice` call fails.
- [ ] **Step 2: run, watch fail. Step 3: implement.** `FileUploadSource.slice` opens the file per call via `vertx.fileSystem().open(...)` with `setReadPos(fromOffset)`, adapts the mutiny `AsyncFile` to `Multi<Buffer>` (`toMulti()`), closing the file on termination. `length()` from `Files.size` at construction. (`AsyncFile` is context-bound — each slice opens fresh on the caller's context, which is the trap-safe pattern.)
- [ ] **Step 4: pass; full build. Step 5: Commit** — `git commit -m "Model upload sources; files are re-readable, one-shots say so"`

---

### Task 7: High level, happy path — `TusClient.upload()` sequential loop + progress

**Files:**
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusClient.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusClientOptions.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusUploadRequest.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/model/TusUploadResult.java`
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/model/TusUploadProgress.java`
- Test: `client-runtime/src/test/java/org/sitenetsoft/quarkus/tus/client/runtime/TusClientTest.java`

**Interfaces:**
- Produces:

```java
public record TusUploadResult(String url, long bytesUploaded) {}
public record TusUploadProgress(long bytesSent, long totalBytes) {}

public final class TusClientOptions {          // builder mirroring config; defaults = config defaults
    public static Builder builder(String url); // .chunkSize .checksumAlgorithm .maxRetries
}                                              // .retryBackoff .retryBackoffMax .parallelism
                                               // .connectTimeout .requestTimeout .customizer

public final class TusUploadRequest {          // builder
    public static Builder builder(UploadSource source);
    // .metadata(Map) .chunkSize(long) .checksumAlgorithm(String) .parallelism(int)
    // .onProgress(Consumer<TusUploadProgress>)
}

public class TusClient {
    public static TusClient create(Vertx vertx, TusClientOptions options);
    public TusProtocolClient protocol();       // the low level, exposed
    public Uni<TusUploadResult> upload(TusUploadRequest request);
    public void close();
}
```

- [ ] **Step 1: failing test** against the scripted server — the whole loop, chunk boundaries and progress:

```java
@Test
void uploadsInChunksAndReportsProgress() {
    // 11 bytes, chunk 4 → create, then PATCH offsets 0,4,8
    enqueueOptions("creation");                       // helper: canned OPTIONS response
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "8")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
    var progress = new ArrayList<TusUploadProgress>();
    var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))  // 11 bytes
            .chunkSize(4).onProgress(progress::add).build()).await().atMost(TIMEOUT);
    assertEquals(11, result.bytesUploaded());
    assertEquals(List.of("OPTIONS", "POST", "PATCH", "PATCH", "PATCH"),
            server.recorded().stream().map(Recorded::method).toList());
    assertEquals("8", server.recorded().get(4).headers().get("Upload-Offset"));
    assertEquals(11L, progress.getLast().bytesSent());
}
```

- [ ] **Step 2: run, watch fail. Step 3: implement.** `upload()` = capability fetch (`options()`, cached in the client per target) → `create` with metadata/length → recursive chunk loop (`Uni` chain: patch slice(offset, min(chunk, remaining)) → recurse until offset == length). Chunk slicing over `UploadSource.slice(offset)` uses a `Multi` limiter that caps emitted bytes at the chunk size (`Multi.select().first(...)` won't cut mid-buffer — write a small `BufferLimiter` operator that truncates the final buffer; unit-test it in the same file: emits exactly N bytes across buffers).
- [ ] **Step 4: pass; full build. Step 5: Commit** — `git commit -m "Upload end to end: create, chunked PATCH loop, progress"`

---

### Task 8: Resume and retry

**Files:** modify `TusClient.java`; test in `TusClientTest.java`.

- [ ] **Step 1: failing tests**

```java
@Test
void transientServerErrorResyncsWithHeadAndResumes() {
    enqueueOptions("creation");
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
    server.enqueue(Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));                       // PATCH @4 dies
    server.enqueue(Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4"))); // HEAD resync
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "8")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
    var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))
            .chunkSize(4).build()).await().atMost(TIMEOUT);
    assertEquals(11, result.bytesUploaded());
    assertEquals("HEAD", server.recorded().get(3).method());   // the resync
}

@Test
void nonRetryableClientErrorFailsFast() {
    enqueueOptions("creation");
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(413, Map.of("Tus-Resumable", "1.0.0")));
    assertThrows(TusPayloadTooLargeException.class, () ->
            client.upload(TusUploadRequest.builder(sourceOf("hello world")).chunkSize(4).build())
                    .await().atMost(TIMEOUT));
    assertEquals(3, server.recorded().size());                 // no retry happened
}

@Test
void retriesAreBoundedByMaxRetries() {
    enqueueOptions("creation");
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    for (int i = 0; i < 4; i++) {                              // 1 try + 3 retries, all 500
        server.enqueue(Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));
        server.enqueue(Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "0")));
    }
    assertThrows(TusServerErrorException.class, () ->
            client.upload(TusUploadRequest.builder(sourceOf("hello world")).chunkSize(4).build())
                    .await().atMost(TIMEOUT));
}

@Test
void oneShotSourceCannotResume() {
    enqueueOptions("creation");
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(500, Map.of("Tus-Resumable", "1.0.0")));
    var failure = assertThrows(TusClientException.class, () ->
            client.upload(TusUploadRequest.builder(
                    UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("hello world")), 11))
                    .chunkSize(4).build()).await().atMost(TIMEOUT));
    assertTrue(failure.getMessage().contains("not replayable"));
}
```

- [ ] **Step 2: run, watch fail. Step 3: implement.** Retryable = `TusServerErrorException` | `TusOffsetMismatchException` | `TusChecksumMismatchException` | I/O failure. On retryable and `source.replayable()`: delay `min(backoff * 2^attempt, backoffMax)` (use `Uni.onItem().delayIt()` — never block), `offset()` resync, continue from the server's offset; give up after `maxRetries`. Non-replayable source + retryable failure → wrap with a message containing "not replayable". Test backoff with tiny durations via `TusClientOptions.retryBackoff(Duration.ofMillis(10))`.
- [ ] **Step 4: pass; full build. Step 5: Commit** — `git commit -m "Resume where the server says: HEAD resync, bounded backoff"`

---

### Task 9: Checksum in the high level

**Files:** modify `TusClient.java`; test in `TusClientTest.java`.

- [ ] **Step 1: failing tests**

```java
@Test
void checksumHeaderCarriesTheChunkDigest() throws Exception {
    enqueueOptions("creation,checksum");                       // helper also sets Tus-Checksum-Algorithm: sha1
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
    client.upload(TusUploadRequest.builder(sourceOf("hello world"))
            .checksumAlgorithm("sha1").build()).await().atMost(TIMEOUT);
    String expected = "sha1 " + Base64.getEncoder().encodeToString(
            MessageDigest.getInstance("SHA-1").digest("hello world".getBytes()));
    assertEquals(expected, server.recorded().get(2).headers().get("Upload-Checksum"));
}

@Test
void checksumAgainstServerWithoutTheExtensionIsRefused() {
    enqueueOptions("creation");                                // no checksum advertised
    var failure = assertThrows(TusCapabilityException.class, () ->
            client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                    .checksumAlgorithm("sha1").build()).await().atMost(TIMEOUT));
    assertEquals("checksum", failure.missingExtension());
}

@Test
void mismatch460RetriesTheChunk() {
    enqueueOptions("creation,checksum");
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(460, Map.of("Tus-Resumable", "1.0.0")));
    server.enqueue(Canned.of(200, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "0")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
    var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))
            .checksumAlgorithm("sha1").build()).await().atMost(TIMEOUT);
    assertEquals(11, result.bytesUploaded());
}
```

- [ ] **Step 2: run, watch fail. Step 3: implement.** With checksum on, the chunk is collected into one `Buffer` first (bounded by chunk size — this is the spec's deliberate bounded buffer), digested (`MessageDigest`, algorithm name mapped `sha1→SHA-1`, `sha256→SHA-256`, `md5→MD5`), then sent as a single-item `Multi` with `TusPatchOptions.checksum(...)`. Checksum off keeps the pure-streaming path from Task 7 — do not collect. Checksum + one-shot source → `TusCapabilityException`-style typed refusal at request build time (message names the conflict). Requested algorithm not in `caps.checksumAlgorithms()` → `TusCapabilityException("checksum")`.
- [ ] **Step 4: pass; full build. Step 5: Commit** — `git commit -m "Digest each chunk and send Upload-Checksum, within one bounded buffer"`

---

### Task 10: Defer-length, including the one-shot ending

**Files:** modify `TusClient.java`; test in `TusClientTest.java`.

- [ ] **Step 1: failing test** — the spec's trailing empty PATCH:

```java
@Test
void oneShotWithUnknownLengthDeclaresItInAFinalEmptyPatch() {
    enqueueOptions("creation,creation-defer-length");
    server.enqueue(Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/u1")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
    server.enqueue(Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "11")));
    var result = client.upload(TusUploadRequest.builder(
            UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("hello world")), -1)).build())
            .await().atMost(TIMEOUT);
    assertEquals(11, result.bytesUploaded());
    var create = server.recorded().get(1);
    assertEquals("1", create.headers().get("Upload-Defer-Length"));
    assertNull(create.headers().get("Upload-Length"));
    var last = server.recorded().getLast();                    // the declaring PATCH
    assertEquals("PATCH", last.method());
    assertEquals("11", last.headers().get("Upload-Length"));
    assertEquals(0, last.body().length());
}
```

- [ ] **Step 2: run, watch fail. Step 3: implement.** `length() == -1` → create with `deferLength()`; stream the source's buffers as chunks as they arrive; at end-of-stream, one final `patch(url, finalOffset, empty Multi, TusPatchOptions.builder().contentLength(0).declareUploadLength(finalOffset).build())`. A replayable source always knows its length, so this path is one-shot-only; guard with capability check for `creation-defer-length`.
- [ ] **Step 4: pass; full build. Step 5: Commit** — `git commit -m "Defer the length until the stream ends, then declare it"`

---

### Task 11: Parallel upload via concatenation

**Files:** modify `TusClient.java`; test in `TusClientTest.java`.

- [ ] **Step 1: failing test**

```java
@Test
void parallelismSplitsIntoPartialsAndConcatenates() {
    enqueueOptions("creation,concatenation");
    // 3 partial creates, 3 partial PATCHes, 1 final concat — order of PATCHes may interleave,
    // so the fixture routes by path: enqueue keyed responses
    server.route("POST", "/tus", Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p1")),
                                 Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p2")),
                                 Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/p3")),
                                 Canned.of(201, Map.of("Tus-Resumable", "1.0.0", "Location", "/tus/fin")));
    server.route("PATCH", "/tus/p1", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
    server.route("PATCH", "/tus/p2", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "4")));
    server.route("PATCH", "/tus/p3", Canned.of(204, Map.of("Tus-Resumable", "1.0.0", "Upload-Offset", "3")));
    var result = client.upload(TusUploadRequest.builder(sourceOf("hello world"))   // 11 bytes / 3
            .parallelism(3).build()).await().atMost(TIMEOUT);
    assertEquals(11, result.bytesUploaded());
    var finalCreate = server.recorded().stream()
            .filter(r -> r.headers().contains("Upload-Concat") && r.headers().get("Upload-Concat").startsWith("final"))
            .findFirst().orElseThrow();
    // ranges in order: p1 gets bytes [0,4), p2 [4,8), p3 [8,11) — final lists them in order
    assertTrue(finalCreate.headers().get("Upload-Concat").endsWith("/tus/p1 " + server.url() + "/p2 " + server.url() + "/p3")
            || finalCreate.headers().get("Upload-Concat").matches("final;.*p1 .*p2 .*p3"));
}

@Test
void parallelismRequiresConcatenationAndAReplayableKnownLengthSource() {
    enqueueOptions("creation");                                // no concatenation
    var failure = assertThrows(TusCapabilityException.class, () ->
            client.upload(TusUploadRequest.builder(sourceOf("hello world"))
                    .parallelism(2).build()).await().atMost(TIMEOUT));
    assertEquals("concatenation", failure.missingExtension());
}
```

Extend `ScriptedTusServer` with `route(method, path, Canned...)` (per-path FIFO taking priority over the global queue) — parallel PATCHes interleave, so global FIFO cannot script them.

- [ ] **Step 2: run, watch fail. Step 3: implement.** Split `[0, length)` into `parallelism` contiguous ranges (last takes the remainder). Each range: `create(partial())` → chunk-loop over `slice` bounded to the range (reuse the Task 7 loop with an end bound). Run ranges with bounded concurrency (`Multi.createFrom().iterable(ranges).onItem().transformToUniAndMerge(...)` with concurrency = parallelism), then `concatenate(urlsInRangeOrder, opts-with-metadata)`. Progress aggregates across partials (`AtomicLong` total). Refuse (typed): parallelism > 1 with one-shot source, unknown length, or missing `concatenation` capability. Per-partial retry reuses Task 8's loop unchanged.
- [ ] **Step 4: pass; full build. Step 5: Commit** — `git commit -m "Upload partials in parallel and concatenate the final"`

---

### Task 12: CDI wiring — `@Inject TusClient` from config

**Files:**
- Create: `client-runtime/src/main/java/org/sitenetsoft/quarkus/tus/client/runtime/TusClientProducer.java`
- Modify: `client-deployment/.../TusClientProcessor.java`
- Modify: `integration-tests/build.gradle` (add `implementation project(':client-runtime')`; Quarkus dev/test augmentation picks up `client-deployment` automatically via the descriptor — if augmentation fails to resolve it in-repo, add `testImplementation project(':client-deployment')` and note it)
- Modify: `integration-tests/src/main/resources/application.properties`
- Test: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/TusClientInjectionTest.java`

**Interfaces:**
- Produces: `@ApplicationScoped TusClientProducer` with `@Produces @ApplicationScoped TusClient tusClient(Vertx vertx, TusClientRuntimeConfig config, Instance<TusRequestCustomizer> customizers)` — maps config → `TusClientOptions` (unset `url` → produce a lazy failing client whose first use throws `TusClientException("quarkus.tus.client.url is not set")`; boot must not fail). At most one customizer bean; more than one → `TusClientException` at first use naming the ambiguity.

- [ ] **Step 1: failing test**

```java
@QuarkusTest
class TusClientInjectionTest {

    @Inject
    TusClient tusClient;

    @Test
    void injectedClientUploadsToTheInRepoServer() {
        var result = tusClient.upload(TusUploadRequest.builder(
                UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("injected!")), 9)).build())
                .await().atMost(Duration.ofSeconds(10));
        assertEquals(9, result.bytesUploaded());
        // offset via the low level proves the server really has it
        assertEquals(9L, tusClient.protocol().offset(result.url()).await().atMost(Duration.ofSeconds(5)));
    }
}
```

application.properties addition:

```properties
quarkus.tus.client.url=http://localhost:${quarkus.http.test-port:8081}/tus
```

- [ ] **Step 2: run, watch fail** (`UnsatisfiedResolutionException` — no bean).
- [ ] **Step 3: implement.** Processor gains `AdditionalBeanBuildItem.unremovableOf(TusClientProducer.class)` and `ReflectiveClassBuildItem.builder(TusUpload.class, TusServerCapabilities.class, TusUploadResult.class, TusUploadProgress.class).methods().fields().build()`. Producer disposes the client on shutdown (`@Disposes` method calling `close()`).
- [ ] **Step 4: pass — `./gradlew :integration-tests:test --tests TusClientInjectionTest`, then full build.**
- [ ] **Step 5: Commit** — `git commit -m "Wire the client into CDI: inject it, configure it, dispose it"`

---

### Task 13: Mutual conformance — the client drives the real server

**Files:**
- Test: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/TusClientConformanceTest.java`

`@QuarkusTest`, programmatic clients (`TusClient.create(vertx, TusClientOptions.builder("http://localhost:" + RestAssured.port + "/tus")...)`) so each test controls its options. Verify stored bytes by reading the upload's data file from the configured upload dir (`quarkus.tus.store.local.upload-dir`, id = URL tail), the same way existing server tests do. **Server chunk limit is 1024 in tests** (`quarkus.tus.max-chunk-size=1024`) — client chunk sizes here stay ≤ 1024.

- [ ] **Step 1: write the suite (failing first is satisfied per-test: write each, run, fix)** — the required cases:

```java
@Test void sequentialUploadArrivesByteForByte()      // 3000 bytes, chunk 512 → file content equals source
@Test void resumeAfterAStreamKilledMidChunk()        // UploadSource whose slice() Multi fails once at byte 700,
                                                     // replayable; client resyncs via HEAD and completes; bytes equal
@Test void checksumGoodAndBad() {
    // good: high level with sha1 → completes, bytes equal
    // bad: low level patch with a deliberately wrong digest → TusChecksumMismatchException,
    //      then offset() shows the server did NOT advance
}
@Test void deferLengthEndsWithTheDeclaringEmptyPatch()  // one-shot, length -1 → completes; HEAD shows final
                                                        // length; pins the server accepting the empty PATCH
@Test void parallelConcatenationReassemblesInOrder()    // parallelism 3, 2500 bytes → final file equals source
@Test void terminateMakesTheUploadGone()                // create+patch, terminate() → offset() throws NotFound
@Test void expirationIsSurfaced()                       // server has expiration on: created upload's
                                                        // expiresAt is present and in the future
@Test void capabilitiesReflectTheServer()               // options() extensions ⊇ creation, termination,
                                                        // checksum, concatenation, creation-defer-length
```

- [ ] **Step 2: run** — `./gradlew :integration-tests:test --tests TusClientConformanceTest`. **Contingency recorded in the spec:** if `deferLengthEndsWithTheDeclaringEmptyPatch` fails because the *server* rejects a zero-byte PATCH carrying `Upload-Length`, that is a server bug this suite exists to catch — fix it in `TusUploadResource`'s PATCH path (the deferred-length branch must process the header before the empty-body short-circuit), with its own failing server test first, in this task.
- [ ] **Step 3: full build green including the whole existing suite.**
- [ ] **Step 4: Commit** — `git commit -m "Prove client and server against each other"`

---

### Task 14: Native image proof, docs, release wiring

**Files:**
- Modify: `integration-tests/src/test/java/org/sitenetsoft/quarkus/tus/it/TusClientInjectionTest.java` (add `@QuarkusIntegrationTest` subclass — follow the existing `*IT` pattern, e.g. `TusClientIT extends` a small HTTP-only base; injection is not available in IT mode, so the IT variant drives a programmatic client against the packaged server)
- Create: `docs/modules/ROOT/pages/client.adoc`
- Modify: `docs/modules/ROOT/pages/index.adoc`

- [ ] **Step 1: IT variant** — `TusClientIT`: programmatic `TusClient` uploads to the packaged app; assert completion via `offset()`. Run `./gradlew :integration-tests:integrationTest`, and if GraalVM is available locally `./gradlew :integration-tests:nativeIntegrationTest` (CI runs it regardless — do not merge before CI native is green).
- [ ] **Step 2: client.adoc** — sections: artifact coordinates + the server/client/both matrix; the two layers with a code example each (the injection example from Task 12 and a low-level relay-flavoured example using `offset`/`patch`/`terminate`); `UploadSource` and the one-shot degradation table (no resume, no checksum, no parallelism — typed errors); checksum's bounded buffer note; parallel upload; `TusRequestCustomizer` auth example; the config table from the spec. Link from `index.adoc`.
- [ ] **Step 3: release wiring check** — `./gradlew publish` (staging): `build/staging-deploy` must contain `org/sitenetsoft/quarkus-tus-client/...` and `quarkus-tus-client-deployment/...` with POMs, sources, javadoc; `unzip -p` the staged runtime jar's `quarkus-extension.properties` and re-verify the deployment-artifact id. The release workflow needs no change (it publishes everything `publishedArtifactIds` covers).
- [ ] **Step 4: full build; Commit** — `git commit -m "Prove the client in native, document it, stage its artifacts"`

---

## Self-review checklist (run after writing, fixed inline)

- Spec coverage: modules/artifacts (T1), dependency rules (T1), both layers (T3–T5, T7), sources (T6), 8/9 scope — creation (T3), creation-with-upload (T3), defer-length (T3, T10), checksum (T4, T9), expiration (T3 parse + T13 surface), concatenation (T5, T11), termination (T5, T13), core resume (T4, T8); capability checks (T9, T11); transport (T3); config (T2, T12); auth customizer (T3, T12); errors/retry/progress (T2, T5, T7, T8); mutual conformance + empty-PATCH pin (T13); fake-server error paths (T3–T5); native (T14); docs (T14); publishing (T1, T14). Follow-ups (tusd, trailer, Dev UI) intentionally absent.
- No placeholders: every step carries code or an exact command.
- Type consistency: `TusUpload(url, offset, length, expiresAt)`, `TusPatchOptions.none()/builder()`, `UploadSource.oneShot(Multi, long)`, `TusCapabilityException.missingExtension()` used identically across tasks.
