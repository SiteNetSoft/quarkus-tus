package org.sitenetsoft.quarkus.tus.deployment;

import io.quarkus.deployment.IsDevelopment;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.devui.spi.JsonRPCProvidersBuildItem;
import io.quarkus.devui.spi.page.CardPageBuildItem;
import io.quarkus.devui.spi.page.Page;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.devui.TusDevUIJsonRpcService;

class TusDevUIProcessor {

    @BuildStep(onlyIf = IsDevelopment.class)
    CardPageBuildItem createDevUICard(TusBuildTimeConfig config) {
        CardPageBuildItem card = new CardPageBuildItem();
        card.addPage(Page.webComponentPageBuilder()
                .title("Uploads")
                .icon("font-awesome-solid:cloud-arrow-up")
                .componentLink("qwc-tus-uploads.js"));
        card.addBuildTimeData("tusPath", config.path());
        card.addBuildTimeData("sseEnabled", config.sseEnabled());
        card.addBuildTimeData("authEnabled", config.authEnabled());
        return card;
    }

    /**
     * Dev UI registers the provider class as an unremovable bean itself, so this is the only
     * registration {@code TusDevUIJsonRpcService} needs — and it exists in dev mode only.
     */
    @BuildStep(onlyIf = IsDevelopment.class)
    JsonRPCProvidersBuildItem registerJsonRpc() {
        return new JsonRPCProvidersBuildItem(TusDevUIJsonRpcService.class);
    }
}
