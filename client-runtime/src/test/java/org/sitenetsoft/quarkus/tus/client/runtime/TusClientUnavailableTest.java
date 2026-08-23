package org.sitenetsoft.quarkus.tus.client.runtime;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException;
import org.sitenetsoft.quarkus.tus.client.runtime.source.UploadSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@link TusClient#unavailable(String)}: the lazy-guard shim the CDI producer returns
 * instead of failing boot when {@code quarkus.tus.client.url} is unset, or when the
 * {@code TusRequestCustomizer} resolution is ambiguous. Boot must succeed; the first real use must
 * fail loudly.
 */
class TusClientUnavailableTest {

    private static final String REASON = "quarkus.tus.client.url is not set";

    /**
     * Guards {@code TusClient#requireAvailable()} in {@code protocol()}. Fails if that call is
     * removed (protocol() would then return null instead of throwing).
     */
    @Test
    void protocolThrowsNamingTheReason() {
        TusClient client = TusClient.unavailable(REASON);

        TusClientException e = assertThrows(TusClientException.class, client::protocol);
        assertTrue(e.getMessage().contains("quarkus.tus.client.url"),
                "expected the message to name quarkus.tus.client.url, was: " + e.getMessage());
    }

    /**
     * Guards {@code TusClient#requireAvailable()} in {@code upload()}. The implementation throws
     * synchronously (before ever returning a Uni), so upload() itself throws rather than returning
     * a failed Uni -- this test asserts that synchronous behavior. Fails if requireAvailable() is
     * removed from upload() (the call would instead NPE on the null `options` field, a different
     * and less informative failure).
     */
    @Test
    void uploadThrowsNamingTheReason() {
        TusClient client = TusClient.unavailable(REASON);
        UploadSource source = UploadSource.oneShot(Multi.createFrom().item(Buffer.buffer("hi")), 2);
        TusUploadRequest request = TusUploadRequest.builder(source).build();

        TusClientException e = assertThrows(TusClientException.class, () -> client.upload(request));
        assertTrue(e.getMessage().contains("quarkus.tus.client.url"),
                "expected the message to name quarkus.tus.client.url, was: " + e.getMessage());
    }

    /**
     * Guards the null-check in {@code TusClient#close()}. Fails with a NullPointerException if that
     * guard is removed, since the unavailable client's `protocol` field is null.
     */
    @Test
    void closeIsASafeNoOp() {
        TusClient client = TusClient.unavailable(REASON);

        assertDoesNotThrow(client::close);
    }
}
