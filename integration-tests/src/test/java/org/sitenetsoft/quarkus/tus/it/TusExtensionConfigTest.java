package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

@QuarkusTest
class TusExtensionConfigTest {

    @Test
    void testTusExtensionHeaderMatchesConfig() {
        given()
                .when().options("/tus")
                .then()
                .statusCode(204)
                .header("Tus-Extension", containsString("creation"))
                .header("Tus-Extension", containsString("termination"))
                .header("Tus-Extension", containsString("checksum"));
    }

    @Test
    void testMaxSizeFromConfig() {
        given()
                .when().options("/tus")
                .then()
                .statusCode(204)
                .header("Tus-Max-Size", "104857600");
    }
}
