# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**quarkus-tus** is a Quarkus extension implementing the [TUS resumable upload protocol](https://tus.io/protocols/resumable-upload). It provides pluggable file upload storage, CDI lifecycle events, optional SSE progress streaming, configurable authentication, and checksum validation.

- **Group:** `org.sitenetsoft`
- **Version:** `1.0.0-SNAPSHOT`
- **License:** Apache 2.0

## Build & Run Commands

| Task | Command |
|---|---|
| Build all modules | `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew build` |
| Run integration tests | `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew :integration-tests:test` |
| Run a single test class | `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew :integration-tests:test --tests org.sitenetsoft.quarkus.tus.it.TusUploadTest` |
| Run a single test method | `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew :integration-tests:test --tests 'org.sitenetsoft.quarkus.tus.it.TusUploadTest.testPatchChunk'` |
| Dev mode (integration-tests) | `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew :integration-tests:quarkusDev` |

**Note:** Java 25 is required. Set `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` for all Gradle commands.

## Tech Stack & Versions

- **Quarkus 3.36.2** (via quarkus-bom)
- **Java 25** (source and target compatibility)
- **Gradle 9.5.1** (wrapper)
- **quarkus-arc**, **quarkus-rest**, **quarkus-scheduler**, **quarkus-vertx** (runtime deps)
- **rest-assured** + **quarkus-junit5** (testing)

## Project Structure (Quarkus Extension)

```
runtime/          — Extension runtime module (io.quarkus.extension plugin)
deployment/       — Extension deployment module (java-library, build steps)
integration-tests/ — @QuarkusTest integration tests (io.quarkus plugin)
```

### Runtime Package Layout (`org.sitenetsoft.quarkus.tus.runtime`)

| Package | Key Classes | Purpose |
|---|---|---|
| `.config` | `TusBuildTimeConfig`, `TusRuntimeConfig` | `@ConfigMapping` configuration |
| `.model` | `UploadInfo`, `UploadProgress` | Upload state POJOs |
| `.event` | `TusUploadCreatedEvent`, `TusChunkReceivedEvent`, `TusUploadCompletedEvent`, `TusUploadTerminatedEvent`, `TusConcatenationCompletedEvent` | CDI event records |
| `.spi` | `UploadStore` | Storage SPI interface |
| `.store` | `LocalFileUploadStore` | Default file-based storage |
| `.sse` | `TusSseService`, `TusSseResource`, `TusProgressResource` | SSE progress streaming (conditional) |
| `.auth` | `TusAuthFilter` | Auth filter (conditional) |
| (root) | `TusUploadResource`, `TusUtils`, `UploadProgressService`, `UploadExpirationScheduler` | Core TUS endpoints and services |

### Deployment Module

`TusProcessor.java` registers beans via `@BuildStep` + `AdditionalBeanBuildItem`. SSE and auth beans are conditionally registered based on `TusBuildTimeConfig`.

## Architecture Notes

- **TUS endpoints** at `/tus`: OPTIONS (capabilities), POST (create), HEAD (status), PATCH (upload chunk), DELETE (terminate)
- **CDI events** fired at each lifecycle point — consumers observe with `@Observes`
- **Storage SPI:** `UploadStore` interface; override `LocalFileUploadStore` via `@Alternative @Priority(1)` for custom backends
- **SSE** endpoints at `/tus/events/{uploadId}` and `/tus/progress/{uploadId}` — conditionally enabled via `quarkus.tus.sse-enabled`
- **Auth filter** — conditionally enabled via `quarkus.tus.auth-enabled`, checks `SecurityContext.getUserPrincipal()`
- **TUS extensions supported:** creation, termination, checksum, expiration, concatenation, creation-with-upload, creation-defer-length
- The `-parameters` compiler flag is set, which Quarkus requires for proper CDI and REST parameter injection

## Configuration Properties

| Property | Phase | Default |
|---|---|---|
| `quarkus.tus.sse-enabled` | Build time | `true` |
| `quarkus.tus.auth-enabled` | Build time | `false` |
| `quarkus.tus.path` | Build time | `/tus` |
| `quarkus.tus.version` | Runtime | `1.0.0` |
| `quarkus.tus.max-size` | Runtime | `107374182400` |
| `quarkus.tus.extensions` | Runtime | `creation,termination,checksum,expiration,concatenation,creation-with-upload,creation-defer-length` |
| `quarkus.tus.expiration-hours` | Runtime | `24` |
| `quarkus.tus.checksum-algorithms` | Runtime | `sha1,md5,sha256` |
| `quarkus.tus.store.local.upload-dir` | Runtime | `${java.io.tmpdir}/quarkus-tus-uploads` |
| `quarkus.tus.max-chunk-size` | Runtime | `10485760` |

## Key Gotchas

- `@ApplicationScoped` beans use CDI client proxies — direct field access bypasses the proxy. Use `@Singleton` or getter methods when test code needs to read bean state directly.
- `@Provider` classes (like `TusAuthFilter`) are auto-discovered by Jandex regardless of `AdditionalBeanBuildItem` conditional registration. The filter must check config at runtime to short-circuit when disabled.
- RESTEasy Reactive consumes the `Content-Type` header for `@Consumes` matching — `@HeaderParam("Content-Type")` returns null. Don't duplicate content-type validation that `@Consumes` already handles.
