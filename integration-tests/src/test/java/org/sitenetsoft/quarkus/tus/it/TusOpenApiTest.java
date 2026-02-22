package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

@QuarkusTest
class TusOpenApiTest {

    @Test
    void testOpenApiContainsTusOperations() {
        given()
                .when().get("/q/openapi")
                .then()
                .statusCode(200)
                .body(containsString("TUS capability discovery"))
                .body(containsString("Query upload status"))
                .body(containsString("Create upload or concatenation"))
                .body(containsString("Upload a chunk of data"))
                .body(containsString("Terminate and delete upload"));
    }
}
