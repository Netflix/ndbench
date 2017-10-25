package com.netflix.ndbench.plugins.janusgraph.configs.cql;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * Specific configs for JanusGraph's CQL backend
 * @author pencal
 */
@Configuration(prefix = "ndbench.config.janusgraph.storage.cql")
public interface ICQLConfig {

    @DefaultValue("ndbench_cql")
    String getKeyspace();

    @DefaultValue("na")
    String getClusterName();
}
