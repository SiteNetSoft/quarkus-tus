package org.sitenetsoft.quarkus.tus.runtime.devui;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;

import jakarta.json.Json;
import jakarta.json.JsonObject;

@ApplicationScoped
public class TusDevUIJsonRpcService {

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Inject
    UploadStore uploadStore;

    public JsonObject getConfig() {
        return Json.createObjectBuilder()
                .add("version", tusRuntimeConfig.version())
                .add("maxSize", tusRuntimeConfig.maxSize())
                .add("maxChunkSize", tusRuntimeConfig.maxChunkSize())
                .add("extensions", tusRuntimeConfig.extensions())
                .add("checksumAlgorithms", tusRuntimeConfig.checksumAlgorithms())
                .add("expirationHours", tusRuntimeConfig.expirationHours())
                .build();
    }

    public String getStoreType() {
        return uploadStore.getClass().getSimpleName();
    }
}
