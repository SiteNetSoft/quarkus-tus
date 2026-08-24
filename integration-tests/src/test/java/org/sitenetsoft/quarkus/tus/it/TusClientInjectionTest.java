package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.sitenetsoft.quarkus.tus.client.runtime.TusClient;

@QuarkusTest
class TusClientInjectionTest extends TusClientTestBase {

    @Inject
    TusClient tusClient;

    @Override
    protected TusClient client() {
        return tusClient;
    }
}
