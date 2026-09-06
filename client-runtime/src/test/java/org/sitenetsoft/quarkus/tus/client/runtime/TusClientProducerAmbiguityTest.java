package org.sitenetsoft.quarkus.tus.client.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@link TusClientProducer#ambiguousCustomizerReason(boolean)}, the pure decision logic
 * behind the producer's ambiguous-{@code TusRequestCustomizer} guard. {@code Instance<T>} is not
 * hand-fakeable without a large stub (it has many default/select methods) and Mockito is not on this
 * module's classpath, so the producer factors the decision into this static, boolean-driven method
 * so it's testable without a CDI container.
 */
class TusClientProducerAmbiguityTest {

    /**
     * Guards the {@code if (!isAmbiguous) return null;} branch. Fails if that early return is
     * removed (a non-ambiguous resolution would then also produce a failure reason, breaking the
     * normal, single-customizer case).
     */
    @Test
    void notAmbiguousProducesNoReason() {
        assertNull(TusClientProducer.ambiguousCustomizerReason(false));
    }

    /**
     * Guards the ambiguous branch itself. Fails if the method is changed to return null
     * unconditionally (which would make the producer silently proceed to
     * {@code customizers.get()} and throw CDI's own ambiguity exception instead of a clear,
     * named {@link org.sitenetsoft.quarkus.tus.client.runtime.error.TusClientException}).
     */
    @Test
    void ambiguousNamesTheProblem() {
        String reason = TusClientProducer.ambiguousCustomizerReason(true);

        assertTrue(reason != null && reason.contains("TusRequestCustomizer"),
                "expected the reason to name TusRequestCustomizer, was: " + reason);
    }

    @Test
    void ambiguousHttpClientCustomizerNamesItsOwnType() {
        assertNull(TusClientProducer.ambiguousHttpClientCustomizerReason(false));
        String reason = TusClientProducer.ambiguousHttpClientCustomizerReason(true);
        assertTrue(reason != null && reason.contains("TusHttpClientCustomizer"),
                "expected the reason to name TusHttpClientCustomizer, was: " + reason);
    }
}
