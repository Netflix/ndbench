/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.ndbench.plugin.janusgraph;

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
