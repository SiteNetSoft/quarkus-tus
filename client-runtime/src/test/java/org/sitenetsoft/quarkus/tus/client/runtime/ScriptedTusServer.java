package org.sitenetsoft.quarkus.tus.client.runtime;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * In-process Vert.x HttpServer for tests: enqueue canned responses, capture what the client
 * actually sent.
 */
final class ScriptedTusServer implements AutoCloseable {

    record Recorded(String method, String path, MultiMap headers, Buffer body) {
    }

    record Canned(int status, Map<String, String> headers, Buffer body) {
        static Canned of(int status, Map<String, String> headers) {
            return new Canned(status, headers, Buffer.buffer());
        }
    }

    private final Vertx vertx;
    private final ConcurrentLinkedDeque<Canned> responses = new ConcurrentLinkedDeque<>();
    private final List<Recorded> recorded = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private int port;

    ScriptedTusServer(Vertx vertx) {
        this.vertx = vertx;
    }

    void enqueue(Canned response) {
        responses.addLast(response);
    }

    List<Recorded> recorded() {
        return recorded;
    }

    String url() {
        return "http://localhost:" + port + "/tus";
    }

    void start() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        server = vertx.createHttpServer().requestHandler(req -> req.bodyHandler(body -> {
            recorded.add(new Recorded(req.method().name(), req.path(),
                    MultiMap.caseInsensitiveMultiMap().addAll(req.headers()), body.copy()));
            Canned canned = responses.pollFirst();
            if (canned == null) {
                req.response().setStatusCode(599).end();
                return;
            }
            var response = req.response().setStatusCode(canned.status());
            canned.headers().forEach(response::putHeader);
            response.end(canned.body());
        }));
        server.listen(0, "localhost", ar -> {
            if (ar.succeeded()) {
                port = ar.result().actualPort();
            }
            latch.countDown();
        });
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("ScriptedTusServer failed to start in time");
        }
        if (server.actualPort() == 0) {
            throw new IllegalStateException("ScriptedTusServer failed to bind");
        }
    }

    @Override
    public void close() {
        if (server == null) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        server.close(ar -> latch.countDown());
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
