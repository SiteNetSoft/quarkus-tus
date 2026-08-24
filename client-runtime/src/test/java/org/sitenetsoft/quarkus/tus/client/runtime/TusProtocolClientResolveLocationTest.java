package org.sitenetsoft.quarkus.tus.client.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TusProtocolClientResolveLocationTest {

    @Test
    void absoluteLocationIsReturnedAsIs() {
        assertEquals("http://other-host:9000/uploads/abc",
                TusProtocolClient.resolveLocation("http://localhost:8080/tus", "http://other-host:9000/uploads/abc"));
    }

    @Test
    void relativeAbsolutePathLocationResolvesAgainstTargetWithoutTrailingSlash() {
        assertEquals("http://localhost:8080/tus/abc",
                TusProtocolClient.resolveLocation("http://localhost:8080/tus", "/tus/abc"));
    }

    @Test
    void relativeAbsolutePathLocationResolvesAgainstTargetWithTrailingSlash() {
        assertEquals("http://localhost:8080/tus/abc",
                TusProtocolClient.resolveLocation("http://localhost:8080/tus/", "/tus/abc"));
    }

    @Test
    void relativeBareLocationResolvesAgainstTargetWithoutTrailingSlash() {
        assertEquals("http://localhost:8080/tus/abc",
                TusProtocolClient.resolveLocation("http://localhost:8080/tus", "abc"));
    }

    @Test
    void relativeBareLocationResolvesAgainstTargetWithTrailingSlash() {
        assertEquals("http://localhost:8080/tus/abc",
                TusProtocolClient.resolveLocation("http://localhost:8080/tus/", "abc"));
    }
}
