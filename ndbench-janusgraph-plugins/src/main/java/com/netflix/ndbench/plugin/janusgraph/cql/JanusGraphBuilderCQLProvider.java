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
package com.netflix.ndbench.plugin.janusgraph.cql;

import com.netflix.ndbench.plugin.janusgraph.IJanusGraphBuilder;
import com.netflix.ndbench.plugin.janusgraph.configs.IJanusGraphConfig;
import com.netflix.ndbench.plugin.janusgraph.configs.cql.ICQLConfig;

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
    protected JanusGraphFactory.Builder graphBuilder;

    public JanusGraphBuilderCQLProvider() {

    }

    @Inject
    public JanusGraphBuilderCQLProvider(IJanusGraphConfig storageConfig, ICQLConfig config) {
        graphBuilder = JanusGraphFactory.build().set("storage.cql.keyspace", config.getKeyspace())
                .set("storage.backend", "cql").set("storage.cql.cluster-name", config.getClusterName())
                .set("storage.hostname", storageConfig.getStorageHostname())
                .set("storage.port", storageConfig.getStoragePort()).set("storage.lock.wait-time", 300)
                .set("cache.db-cache", false).set("query.batch", false).set("query.smart-limit", false)
                .set("query.force-index", false).set("query.fast-property", false);
    }

    public JanusGraphFactory.Builder getGraphBuilder() {
        return graphBuilder;
    }
}
