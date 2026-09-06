package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.ValidatableResponse;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The PATCH that turns a deferred length into a known one runs under the upload's lock, and it
 * is the one place where the resource itself mutates the record and fires events. What it
 * accepts, and what happens to the lock when something on that path fails, is covered here.
 */
@QuarkusTest
class TusDeferLengthDeclarationTest {

    private static String createUpload(long size) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(size))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    private static void uploadData(String location, byte[] data, long offset) {
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", String.valueOf(offset))
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    private static String extractId(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    @Inject
    TusTestObserver observer;

    @AfterEach
    void resetObserver() {
        observer.reset();
    }

    private static String createDeferred() {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Defer-Length", "1")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    private static ValidatableResponse declare(String location, long offset, long length) {
        return given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", String.valueOf(offset))
                .header("Upload-Length", String.valueOf(length))
                .contentType("application/offset+octet-stream")
                .when().patch(location)
                .then();
    }

    /**
     * A declaring PATCH that also completes the upload fires the completion event from under the
     * lock. An application observer that throws there used to leave the lock held for good — the
     * per-branch releases did not cover a failure after the record was updated — so every
     * later PATCH answered 423.
     */
    @Test
    void observerFailureOnTheDeclaringPatchReleasesTheLock() {
        String location = createDeferred();
        String id = extractId(location);
        uploadData(location, new byte[10], 0);

        observer.failCompletionFor.add(id);
        declare(location, 10, 10).statusCode(500);
        observer.failCompletionFor.remove(id);

        // Not 423: the lock went with the failed request.
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "10")
                .contentType("application/offset+octet-stream")
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    /** Observers are application code; like every other event, completion is delivered off the event loop. */
    @Test
    void completionOnTheDeclaringPatchIsFiredOffTheEventLoop() {
        String location = createDeferred();
        String id = extractId(location);
        uploadData(location, new byte[10], 0);
        declare(location, 10, 10).statusCode(204);

        String thread = observer.completionThreads.get(id);
        assertNotNull(thread, "completion event was not fired");
        assertFalse(thread.startsWith("vert.x-eventloop"),
                "completion event fired on the event loop: " + thread);
    }

    /**
     * A length below what has already been stored contradicts the bytes on disk. It is refused
     * before the record is touched, so the length stays deferred and a correct one still works.
     */
    @Test
    void declaredLengthBelowTheCurrentOffsetIsRejected() {
        String location = createDeferred();
        uploadData(location, new byte[10], 0);

        declare(location, 10, 5).statusCode(400);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Defer-Length", "1")
                .header("Upload-Length", nullValue())
                .header("Upload-Offset", "10");

        declare(location, 10, 10).statusCode(204);
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Length", "10");
    }
}
