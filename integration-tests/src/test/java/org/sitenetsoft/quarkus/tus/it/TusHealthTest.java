package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

@QuarkusTest
class TusHealthTest {

    @Test
    void testReadinessCheckIncludesTusStore() {
        given()
                .when().get("/q/health/ready")
                .then()
                .statusCode(200)
                .body(containsString("TUS Upload Store"))
                .body(containsString("UP"));
    }
}
