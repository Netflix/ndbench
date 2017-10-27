package com.netflix.ndbench.plugins.janusgraph.cql;

import com.netflix.ndbench.plugins.janusgraph.IJanusGraphBuilder;
import com.netflix.ndbench.plugins.janusgraph.configs.IJanusGraphConfig;
import com.netflix.ndbench.plugins.janusgraph.configs.cql.ICQLConfig;
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
    public JanusGraphBuilderCQLProvider(IJanusGraphConfig storageConfig, ICQLConfig config) {
        graphBuilder = JanusGraphFactory
                .build()
                .set("storage.cql.keyspace", config.getKeyspace())
                .set("storage.backend", "cql")
                .set("storage.cql.cluster-name", config.getClusterName())
                .set("storage.hostname", storageConfig.getStorageHostname())
                .set("storage.port", storageConfig.getStoragePort())
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
