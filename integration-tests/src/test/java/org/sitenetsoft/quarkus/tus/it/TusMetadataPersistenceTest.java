package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class TusMetadataPersistenceTest {

    @ConfigProperty(name = "quarkus.tus.store.local.upload-dir")
    String uploadDir;

    @Inject
    UploadStore uploadStore;

    // ---- Restart reconciliation ----

    /**
     * Staged bytes reach the data file before the framework has verified them, so a crash
     * between stage and commit (or abort) leaves the file longer than the persisted offset.
     * Only committed offsets are ever persisted; a restart must cut the file back to that,
     * never adopt the unverified tail as if it had been acknowledged.
     */
    @Test
    void testReloadTruncatesDataFileLongerThanPersistedOffset() throws IOException {
        String id = java.util.UUID.randomUUID().toString();
        Path data = Path.of(uploadDir, id);
        Files.write(data, new byte[500]);
        UploadInfo info = new UploadInfo();
        info.setEntityLength(1000L);
        info.setOffset(200L);
        Files.writeString(Path.of(uploadDir, id + ".meta"), info.toJson());

        ((LocalFileUploadStore) uploadStore).reloadPersistedUploads();

        assertEquals(200L, uploadStore.findUploadInfo(id).orElseThrow().getOffset());
        assertEquals(200L, Files.size(data), "unverified staged tail must be cut off");
        uploadStore.discardUpload(id);
    }

    /** A file shorter than the persisted offset has really lost data; the file is the truth then. */
    @Test
    void testReloadFallsBackToFileSizeWhenDataFileIsShorter() throws IOException {
        String id = java.util.UUID.randomUUID().toString();
        Path data = Path.of(uploadDir, id);
        Files.write(data, new byte[100]);
        UploadInfo info = new UploadInfo();
        info.setEntityLength(1000L);
        info.setOffset(300L);
        Files.writeString(Path.of(uploadDir, id + ".meta"), info.toJson());

        ((LocalFileUploadStore) uploadStore).reloadPersistedUploads();

        assertEquals(100L, uploadStore.findUploadInfo(id).orElseThrow().getOffset());
        uploadStore.discardUpload(id);
    }

    // ---- JSON round-trip ----

    @Test
    void testUploadInfoJsonRoundTrip() {
        UploadInfo info = new UploadInfo();
        info.setEntityLength(12345L);
        info.setOffset(5000L);
        info.setMetadata("filename dGVzdC50eHQ=");
        info.setPartial(true);
        info.setUploadConcatMergedValue("final;/tus/a /tus/b");
        info.setExpiresAt(Instant.parse("2025-06-01T12:00:00Z"));
        info.setDeferredLength(false);
        info.setFinalConcat(false);
        info.setPartialIds(List.of("id-1", "id-2"));
        info.setUploaderId("user42");

        String json = info.toJson();
        UploadInfo restored = UploadInfo.fromJson(json);

        assertEquals(info.getEntityLength(), restored.getEntityLength());
        assertEquals(info.getOffset(), restored.getOffset());
        assertEquals(info.getMetadata(), restored.getMetadata());
        assertEquals(info.isPartial(), restored.isPartial());
        assertEquals(info.getUploadConcatMergedValue(), restored.getUploadConcatMergedValue());
        assertEquals(info.getExpiresAt(), restored.getExpiresAt());
        assertEquals(info.isDeferredLength(), restored.isDeferredLength());
        assertEquals(info.isFinalConcat(), restored.isFinalConcat());
        assertEquals(info.getPartialIds(), restored.getPartialIds());
        assertEquals(info.getUploaderId(), restored.getUploaderId());
    }

    @Test
    void testOlderMetadataWithRetiredFieldsStillLoads() {
        // Metadata written by earlier versions carried a completion latch; it is ignored now.
        UploadInfo restored = UploadInfo.fromJson(
                "{\"entityLength\":10,\"offset\":0,\"isPartial\":false,"
                        + "\"deferredLength\":false,\"isFinalConcat\":false,\"completionFired\":true}");

        assertEquals(10, restored.getEntityLength());
        assertEquals(0, restored.getOffset());
    }

    // ---- .meta file created on upload creation ----

    @Test
    void testMetaFileCreatedOnUploadCreation() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        String id = extractId(location);
        Path metaFile = Path.of(uploadDir, id + ".meta");
        assertTrue(Files.exists(metaFile), ".meta file should exist after creation");
    }

    // ---- .meta file deleted on discard ----

    @Test
    void testMetaFileDeletedOnDiscard() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String id = extractId(location);
        Path metaFile = Path.of(uploadDir, id + ".meta");
        assertTrue(Files.exists(metaFile), ".meta file should exist before delete");

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        assertFalse(Files.exists(metaFile), ".meta file should be removed after delete");
    }

    // ---- .meta file updated on PATCH ----

    @Test
    void testMetaFileUpdatedOnPatch() throws IOException {
        byte[] data = "hello".getBytes();
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String id = extractId(location);
        Path metaFile = Path.of(uploadDir, id + ".meta");

        // Read initial meta — offset should be 0
        String initialJson = Files.readString(metaFile);
        UploadInfo initial = UploadInfo.fromJson(initialJson);
        assertEquals(0, initial.getOffset());

        // PATCH
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);

        // Read updated meta — offset should match data length
        String updatedJson = Files.readString(metaFile);
        UploadInfo updated = UploadInfo.fromJson(updatedJson);
        assertEquals(data.length, updated.getOffset());
    }

    // ---- Deferred upload .meta persisted ----

    @Test
    void testDeferredUploadMetaPersisted() throws IOException {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String id = extractId(location);
        Path metaFile = Path.of(uploadDir, id + ".meta");
        assertTrue(Files.exists(metaFile));

        String json = Files.readString(metaFile);
        UploadInfo info = UploadInfo.fromJson(json);
        assertTrue(info.isDeferredLength(), "Deferred flag should be true in .meta");
        assertEquals(-1, info.getEntityLength(), "Entity length should be -1 for deferred");
    }

    // ---- Concatenation .meta persisted ----

    @Test
    void testConcatenationMetaPersisted() throws IOException {
        // Create two partial uploads
        byte[] data1 = "part1".getBytes();
        byte[] data2 = "part2".getBytes();

        String loc1 = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data1.length))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then().statusCode(201).extract().header("Location");

        String loc2 = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data2.length))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then().statusCode(201).extract().header("Location");

        // Upload data to both partials
        given().header("Tus-Resumable", "1.0.0").header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream").body(data1)
                .when().patch(loc1).then().statusCode(204);

        given().header("Tus-Resumable", "1.0.0").header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream").body(data2)
                .when().patch(loc2).then().statusCode(204);

        // Merge
        String finalLocation = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", "final; " + loc1 + " " + loc2)
                .when().post("/tus")
                .then().statusCode(201).extract().header("Location");

        String finalId = extractId(finalLocation);
        Path metaFile = Path.of(uploadDir, finalId + ".meta");
        assertTrue(Files.exists(metaFile), "Final concat .meta should exist");

        String json = Files.readString(metaFile);
        UploadInfo info = UploadInfo.fromJson(json);
        assertNotNull(info.getUploadConcatMergedValue(), "Upload-Concat value should be persisted");
        assertTrue(info.getUploadConcatMergedValue().startsWith("final;"));
    }

    private String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }
}
