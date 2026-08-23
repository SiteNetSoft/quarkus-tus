package org.sitenetsoft.quarkus.tus.client.runtime.model;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

public record TusServerCapabilities(List<String> versions, Set<String> extensions,
                                    OptionalLong maxSize, Set<String> checksumAlgorithms) {
    public boolean supports(String extension) {
        return extensions.contains(extension);
    }
}
