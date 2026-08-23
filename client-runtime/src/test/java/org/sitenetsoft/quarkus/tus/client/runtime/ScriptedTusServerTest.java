package org.sitenetsoft.quarkus.tus.client.runtime;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScriptedTusServerTest {

    private Vertx vertx;
    private ScriptedTusServer server;

    @BeforeEach
    void setUp() throws InterruptedException {
        vertx = Vertx.vertx();
        server = new ScriptedTusServer(vertx);
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.close();
        vertx.close();
    }

    @Test
    void recordsWhatWasSentAndAnswersFromTheQueue() throws Exception {
        server.enqueue(ScriptedTusServer.Canned.of(204, Map.of("X-Test", "yes")));

        URI uri = URI.create(server.url());
        HttpClient client = vertx.createHttpClient();
        CompletableFuture<Integer> statusFuture = new CompletableFuture<>();
        RequestOptions options = new RequestOptions()
                .setMethod(HttpMethod.POST)
                .setAbsoluteURI(server.url());
        client.request(options)
                .onSuccess(req -> {
                    req.putHeader("X-Sent", "hello");
                    req.send(Buffer.buffer("body-content"))
                            .onSuccess(resp -> statusFuture.complete(resp.statusCode()))
                            .onFailure(statusFuture::completeExceptionally);
                })
                .onFailure(statusFuture::completeExceptionally);

        int status = statusFuture.get(10, TimeUnit.SECONDS);
        assertEquals(204, status);

        var recorded = server.recorded().getFirst();
        assertEquals("POST", recorded.method());
        assertEquals("/tus", recorded.path());
        assertEquals("hello", recorded.headers().get("X-Sent"));
        assertEquals("body-content", recorded.body().toString());

        client.close();
    }

    @Test
    void answers599WhenTheQueueIsEmpty() throws Exception {
        HttpClient client = vertx.createHttpClient();
        CompletableFuture<Integer> statusFuture = new CompletableFuture<>();
        RequestOptions options = new RequestOptions()
                .setMethod(HttpMethod.GET)
                .setAbsoluteURI(server.url());
        client.request(options)
                .onSuccess(req -> req.send()
                        .onSuccess(resp -> statusFuture.complete(resp.statusCode()))
                        .onFailure(statusFuture::completeExceptionally))
                .onFailure(statusFuture::completeExceptionally);

        assertEquals(599, statusFuture.get(10, TimeUnit.SECONDS));
        client.close();
    }
}
