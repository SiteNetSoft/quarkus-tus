package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;

/**
 * Authorization tests: an authenticated user must not be able to read, modify,
 * delete or concatenate another user's uploads.
 */
@QuarkusTest
@TestProfile(TusOwnershipTest.AuthEnabledProfile.class)
class TusOwnershipTest {

    public static class AuthEnabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "quarkus.tus.auth-enabled", "true",
                    "quarkus.http.auth.basic", "true",
                    "quarkus.security.users.embedded.enabled", "true",
                    "quarkus.security.users.embedded.plain-text", "true",
                    "quarkus.security.users.embedded.users.alice", "alice-pw",
                    "quarkus.security.users.embedded.users.bob", "bob-pw",
                    "quarkus.security.users.embedded.roles.alice", "user",
                    "quarkus.security.users.embedded.roles.bob", "user");
        }
    }

    private static RequestSpecification asAlice() {
        return given().auth().preemptive().basic("alice", "alice-pw")
                .header("Tus-Resumable", "1.0.0");
    }

    private static RequestSpecification asBob() {
        return given().auth().preemptive().basic("bob", "bob-pw")
                .header("Tus-Resumable", "1.0.0");
    }

    private static String aliceCreatesUpload(int length) {
        return asAlice()
                .header("Upload-Length", String.valueOf(length))
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    private static String aliceCreatesPartial(int length) {
        return asAlice()
                .header("Upload-Length", String.valueOf(length))
                .header("Upload-Concat", "partial")
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");
    }

    private static String aliceCreatesCompletePartial(byte[] data) {
        String location = aliceCreatesPartial(data.length);
        asAlice()
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body(data)
                .when().patch(location)
                .then()
                .statusCode(204);
        return location;
    }

    // ---- The owner keeps full access ----

    @Test
    void ownerCanHeadOwnUpload() {
        String location = aliceCreatesUpload(100);

        asAlice()
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", "0");
    }

    @Test
    void ownerCanDeleteOwnUpload() {
        String location = aliceCreatesUpload(100);

        asAlice()
                .when().delete(location)
                .then()
                .statusCode(204);
    }

    // ---- Cross-user access is denied ----

    @Test
    void otherUserCannotHeadUpload() {
        String location = aliceCreatesUpload(100);

        asBob()
                .when().head(location)
                .then()
                .statusCode(404);
    }

    @Test
    void otherUserCannotPatchUpload() {
        String location = aliceCreatesUpload(100);

        asBob()
                .header("Upload-Offset", "0")
                .contentType("application/offset+octet-stream")
                .body("intruder".getBytes())
                .when().patch(location)
                .then()
                .statusCode(404);

        // Alice's upload must be untouched.
        asAlice()
                .when().head(location)
                .then()
                .statusCode(200)
                .header("Upload-Offset", "0");
    }

    @Test
    void otherUserCannotDeleteUpload() {
        String location = aliceCreatesUpload(100);

        asBob()
                .when().delete(location)
                .then()
                .statusCode(404);

        // The upload must still exist for its owner.
        asAlice()
                .when().head(location)
                .then()
                .statusCode(200);
    }

    /**
     * X-HTTP-Method-Override rewrites the method before resource matching, so the ownership
     * check must still see the effective method rather than the wire method.
     */
    @Test
    void otherUserCannotDeleteViaMethodOverride() {
        String location = aliceCreatesUpload(100);

        asBob()
                .header("X-HTTP-Method-Override", "DELETE")
                .when().post(location)
                .then()
                .statusCode(404);

        asAlice()
                .when().head(location)
                .then()
                .statusCode(200);
    }

    // ---- Concatenation must not be a way around the ownership check ----

    @Test
    void otherUserCannotConcatenateCompletePartials() {
        String loc1 = aliceCreatesCompletePartial("aaa".getBytes());
        String loc2 = aliceCreatesCompletePartial("bbb".getBytes());

        asBob()
                .header("Upload-Concat", "final; " + loc1 + " " + loc2)
                .when().post("/tus")
                .then()
                .statusCode(400);

        // Alice's partials must survive the attempt.
        asAlice().when().head(loc1).then().statusCode(200);
        asAlice().when().head(loc2).then().statusCode(200);
    }

    /**
     * Regression test for the concatenation ownership bypass: when the ownership-checked
     * merge refused, the request used to fall through to an unfinished-merge path that had
     * no ownership check at all. Incomplete partials are what take that second path.
     */
    @Test
    void otherUserCannotConcatenateIncompletePartials() {
        String loc = aliceCreatesPartial(50);

        asBob()
                .header("Upload-Concat", "final; " + loc)
                .when().post("/tus")
                .then()
                .statusCode(400);

        asAlice().when().head(loc).then().statusCode(200);
    }

    @Test
    void ownerCanConcatenateOwnPartials() {
        String loc1 = aliceCreatesCompletePartial("aaa".getBytes());
        String loc2 = aliceCreatesCompletePartial("bbb".getBytes());

        String finalLocation = asAlice()
                .header("Upload-Concat", "final; " + loc1 + " " + loc2)
                .when().post("/tus")
                .then()
                .statusCode(201)
                .extract().header("Location");

        asAlice()
                .when().head(finalLocation)
                .then()
                .statusCode(200)
                .header("Upload-Length", "6");
    }
}
