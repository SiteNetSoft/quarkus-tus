# Quarkus TUS

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A [Quarkus](https://quarkus.io/) extension implementing the [TUS resumable upload protocol](https://tus.io/protocols/resumable-upload) (v1.0.0).

## Features

- Full TUS v1.0.0 protocol implementation (creation, termination, checksum, expiration, concatenation, creation-with-upload, creation-defer-length)
- Pluggable storage backends via SPI (`UploadStore` interface)
- CDI lifecycle events for upload created, chunk received, upload completed, upload terminated, and concatenation completed
- Optional SSE (Server-Sent Events) for real-time upload progress
- Optional authentication filter
- Checksum validation (SHA-1, MD5, SHA-256)
- Automatic expiration of incomplete uploads

## Getting Started

### Installation

Add the extension to your Quarkus application:

#### Gradle

```kotlin
implementation("org.sitenetsoft:quarkus-tus:1.0.0")
```

#### Maven

```xml
<dependency>
    <groupId>org.sitenetsoft</groupId>
    <artifactId>quarkus-tus</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Minimal Configuration

The extension works out of the box with sensible defaults. Add to `application.properties` to customize:

```properties
# Max upload size (default: 100 GB)
quarkus.tus.max-size=1073741824

# Upload storage directory (default: ${java.io.tmpdir}/quarkus-tus-uploads)
quarkus.tus.store.local.upload-dir=/var/uploads

# Expiration for incomplete uploads in hours (default: 24)
quarkus.tus.expiration-hours=48
```

### TUS Endpoints

The extension registers the following endpoints at the configured path (default `/tus`):

| Method | Path | Description |
|---|---|---|
| `OPTIONS` | `/tus` | TUS capability discovery (protocol version, extensions, max size, checksum algorithms) |
| `POST` | `/tus` | Create a new upload (supports creation-with-upload and concatenation) |
| `HEAD` | `/tus/{id}` | Query upload status (offset, length, expiration) |
| `PATCH` | `/tus/{id}` | Upload a chunk of data (resumable) |
| `DELETE` | `/tus/{id}` | Terminate and delete an upload |

### Quick Example: Observing Upload Events

```java
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.sitenetsoft.quarkus.tus.runtime.event.*;

@ApplicationScoped
public class UploadEventHandler {

    void onCreated(@Observes TusUploadCreatedEvent event) {
        Log.infof("Upload started: %s (%d bytes)", event.uploadId(), event.totalSize());
    }

    void onCompleted(@Observes TusUploadCompletedEvent event) {
        Log.infof("Upload finished: %s", event.uploadId());
        // Process the completed file...
    }
}
```

## Building from Source

Requires **Java 25** and **Gradle 9.3.1** (wrapper included).

```bash
# Build all modules
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew build

# Run @QuarkusTest integration tests only
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew :integration-tests:test

# Run @QuarkusIntegrationTest tests against the packaged JAR
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 ./gradlew :integration-tests:integrationTest
```

## Documentation

Read the [full documentation](docs/modules/ROOT/pages/index.adoc) for detailed guides on:

- [Configuration Reference](docs/modules/ROOT/pages/configuration.adoc)
- [CDI Lifecycle Events](docs/modules/ROOT/pages/cdi-events.adoc)
- [Custom Storage Backends](docs/modules/ROOT/pages/storage-spi.adoc)
- [SSE Upload Progress](docs/modules/ROOT/pages/sse.adoc)
- [Authentication](docs/modules/ROOT/pages/authentication.adoc)
- [Testing](docs/modules/ROOT/pages/testing.adoc)

## Compatibility

| Extension Version | Quarkus Version | Java Version |
|---|---|---|
| 1.0.0 | 3.38.0 | 25 |
| 0.1.0 | 3.38.0 | 25 |

### Stability

The TUS endpoints and configuration are stable: they implement a published protocol and are covered
by a conformance suite.

The `UploadStore` SPI has been redesigned for `1.0.0` and is not compatible with `0.1.0`: chunk
bodies now stream through the store as a backpressured `Multi<Buffer>` via a staged
`stageChunk`/`commitChunk`/`abortChunk` write, so an object-store backend can pipe bytes to S3 as they
arrive, and every protocol concern (checksums, events, validation, Location building) has moved out
of the store. A contract test, `org.sitenetsoft:quarkus-tus-tck`, lets a backend prove it honours
the SPI. See [Custom Storage Backends](docs/modules/ROOT/pages/storage-spi.adoc).

## License

This project is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
