package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;

@QuarkusTest
class TusMetricsTest {

    @Test
    void testMetricsAfterUploadCycle() {
        byte[] data = "metrics test".getBytes();

        // Create upload
        String location = given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Length", String.valueOf(data.length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .header("Location", notNullValue())
                .extract().header("Location");

        // Upload data
        given()
                .header("Tus-Resumable", "1.0.0")
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);

        // Delete upload
        given()
                .header("Tus-Resumable", "1.0.0")
                .when().delete(location)
                .then()
                .statusCode(204);

        // Verify metrics
        given()
                .when().get("/q/metrics")
                .then()
                .statusCode(200)
                .body(containsString("tus_uploads_created_total"))
                .body(containsString("tus_uploads_completed_total"))
                .body(containsString("tus_uploads_terminated_total"))
                .body(containsString("tus_chunks_received_total"));
    }
}
