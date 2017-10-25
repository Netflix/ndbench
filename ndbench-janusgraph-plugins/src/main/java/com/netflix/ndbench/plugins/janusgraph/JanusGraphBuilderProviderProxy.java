package com.netflix.ndbench.plugins.janusgraph;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.plugins.janusgraph.cql.JanusGraphBuilderCQLProvider;
import org.janusgraph.core.JanusGraphFactory;

/**
 * Proxy class to determine which JanusGraphBuilderProviders to use based on the property "ndbench.config.janusgraph.storage.backend". i.e. CQL, Cassandra, Hbase, BigTable etc.
 * Currently only CQL is supported, though new storage backend can be added easily
 *
 * @author pencal
 */
@Singleton
public class JanusGraphBuilderProviderProxy {
    private final IJanusGraphBuilder graphBuilder;

    @Inject
    public JanusGraphBuilderProviderProxy(PropertyFactory propertyFactory, JanusGraphBuilderCQLProvider cqlProvider) {
        String backend = propertyFactory.getProperty("ndbench.config.janusgraph.storage.backend").asString("N/A").get();

        switch (backend.toLowerCase()) {
            case "cql":
                this.graphBuilder = cqlProvider;
                break;
            default:
                throw new IllegalArgumentException("unsupported JanusGraph backend. Check the config value - ndbench.config.janusgraph.storage.backend");
        }
    }

    public JanusGraphFactory.Builder getGraphBuilder() {
        return graphBuilder.getGraphBuilder();
    }

}
