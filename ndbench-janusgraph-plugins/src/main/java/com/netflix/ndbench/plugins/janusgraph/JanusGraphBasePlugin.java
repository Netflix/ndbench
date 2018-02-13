package com.netflix.ndbench.plugins.janusgraph;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

/**
 * Common logic for all JanusGraph plugins, regardless of which storage backend being used.
 *
 * @author pencal
 */
public abstract class JanusGraphBasePlugin {
    static final String COMPOSITE_INDEX_NAME = "idx_customId";
    static final String VERTEX_LABEL_LEVEL_1 = "level1";
    static final String OK = "ok";
    static final String CACHE_MISS = null;
    static final String PROP_CUSTOM_ID_KEY = "prop_customId";
    static final String PROP_METADATA_KEY = "metadata";

    final String storageBackend;
    final String storageHost;
    final String storagePort;

    public JanusGraphBasePlugin(String backend, String host, String port) {
        this.storageBackend = backend;
        this.storageHost = host;
        this.storagePort = port;
    }

    protected void createSchema(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        if (!mgmt.containsGraphIndex(COMPOSITE_INDEX_NAME)) {
            final PropertyKey customId = mgmt.makePropertyKey(PROP_CUSTOM_ID_KEY).dataType(String.class).make();
            JanusGraphManagement.IndexBuilder customIdIndexBuilder = mgmt.buildIndex(COMPOSITE_INDEX_NAME, Vertex.class).addKey(customId);
            customIdIndexBuilder.buildCompositeIndex();

            mgmt.makeVertexLabel(VERTEX_LABEL_LEVEL_1).make();
            mgmt.commit();
        }
    }

    protected String getConnectionInfo(JanusGraph graph) {
        String status = graph.isOpen() ? "opened" : "closed";
        return String.format("Backend: %s, Host: %s, Port: %s, Graph Status: %s",
                storageBackend,
                storageHost,
                storagePort,
                status);
    }
}
