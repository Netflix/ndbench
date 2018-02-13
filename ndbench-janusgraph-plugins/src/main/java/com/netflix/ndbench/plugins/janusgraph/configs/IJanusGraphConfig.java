package com.netflix.ndbench.plugins.janusgraph.configs;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * Common configs for JanusGraph benchmark
 *
 * @author pencal
 */
@Configuration(prefix = "ndbench.config.janusgraph")
public interface IJanusGraphConfig {
    // One can benchmark either the Tinkerpop API or the JanusGraph Core API if needed
    @DefaultValue("false")
    boolean useJanusgraphTransaction();

    @DefaultValue("127.0.0.1")
    String getStorageHostname();

    @DefaultValue("9042")
    String getStoragePort();
}
