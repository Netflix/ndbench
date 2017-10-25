package com.netflix.ndbench.plugins.janusgraph.cql;

import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.plugins.janusgraph.IJanusGraphBuilder;
import org.janusgraph.core.JanusGraphFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/***
 * Provides a JanusGraph builder backed by CQL
 *
 * @author pencal
 */
@Singleton
public class JanusGraphBuilderCQLProvider implements IJanusGraphBuilder {
    private final JanusGraphFactory.Builder graphBuilder;

    @Inject
    public JanusGraphBuilderCQLProvider(PropertyFactory factory) {
        graphBuilder = JanusGraphFactory
                .build()
                .set("storage.cql.keyspace", factory.getProperty("ndbench.config.janusgraph.storage.cql.keyspace").asString("").get())
                .set("storage.backend", "cql")
                .set("storage.cql.cluster-name", factory.getProperty("ndbench.config.janusgraph.storage.cql.cluster-name").asString("").get())
                .set("storage.hostname", factory.getProperty("ndbench.config.janusgraph.storage.hostname").asString("").get())
                .set("storage.port", factory.getProperty("ndbench.config.janusgraph.storage.port").asString("").get())
                .set("storage.lock.wait-time", 300)
                .set("cache.db-cache", false)
                .set("query.batch", false)
                .set("query.smart-limit", false)
                .set("query.force-index", false)
                .set("query.fast-property", false);
    }

    public JanusGraphFactory.Builder getGraphBuilder() {
        return graphBuilder;
    }
}
