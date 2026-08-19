package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.Set;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(TusCustomStoreTest.CustomStoreProfile.class)
class TusCustomStoreTest {

    public static class CustomStoreProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(InMemoryUploadStore.class);
        }
    }

    @Inject
    UploadStore uploadStore;

    @Test
    void testInjectedStoreIsInMemory() {
        assertInstanceOf(InMemoryUploadStore.class, uploadStore);
    }

    @Test
    void testFullUploadCycleThroughCustomStore() {
        byte[] data = "custom store data".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204)
                .header("Upload-Offset", String.valueOf(data.length));

        InMemoryUploadStore inMemStore = (InMemoryUploadStore) uploadStore;
        assertTrue(inMemStore.hasData(uploadId), "Data should exist in in-memory store");
        assertArrayEquals(data, inMemStore.getData(uploadId), "Stored data should match uploaded data");
    }

    @Test
    void testChecksumThroughCustomStore() throws Exception {
        byte[] data = "checksum in custom store".getBytes();
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        String checksum = "sha1 " + Base64.getEncoder().encodeToString(md.digest(data));

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .header("Upload-Checksum", checksum)
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
    }

    @Test
    void testDeleteThroughCustomStore() {
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", "100")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String uploadId = location.substring(location.lastIndexOf('/') + 1);

        InMemoryUploadStore inMemStore = (InMemoryUploadStore) uploadStore;
        assertTrue(inMemStore.hasData(uploadId), "Data should exist before delete");

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        assertFalse(inMemStore.hasData(uploadId), "Data should be removed after delete");
        assertTrue(Stores.find(uploadStore, uploadId).isEmpty(), "Upload info should be removed after delete");
    }

    @Test
    void testConcatenationThroughCustomStore() {
        byte[] data1 = "part one".getBytes();
        byte[] data2 = " part two".getBytes();

        // Create partial uploads
        String loc1 = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data1.length))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String loc2 = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data2.length))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        // Upload data to partials
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data1)
                .when().patch(loc1)
                .then()
                .statusCode(204);

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data2)
                .when().patch(loc2)
                .then()
                .statusCode(204);

        // Create final concatenation
        String finalLocation = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Concat", "final; " + loc1 + " " + loc2)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        String finalId = finalLocation.substring(finalLocation.lastIndexOf('/') + 1);

        // Verify merged data in InMemoryUploadStore
        InMemoryUploadStore inMemStore = (InMemoryUploadStore) uploadStore;
        assertTrue(inMemStore.hasData(finalId), "Final upload data should exist");

        byte[] expected = new byte[data1.length + data2.length];
        System.arraycopy(data1, 0, expected, 0, data1.length);
        System.arraycopy(data2, 0, expected, data1.length, data2.length);
        assertArrayEquals(expected, inMemStore.getData(finalId), "Merged data should equal concatenation of partials");
    }

    @Test
    void testHeadThroughCustomStore() {
        byte[] data = "head test data".getBytes();

        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);

        given()
                .header("Tus-Resumable", "1.0.0")
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", String.valueOf(data.length))
                .header("Upload-Length", String.valueOf(data.length));
    }
}
