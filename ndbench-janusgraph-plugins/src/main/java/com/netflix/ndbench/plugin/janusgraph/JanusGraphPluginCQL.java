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

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.janusgraph.configs.IJanusGraphConfig;
import com.netflix.ndbench.plugin.janusgraph.cql.JanusGraphBuilderCQLProvider;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/***
 * JanusGraph benchmarking plugin to measure throughput of write and read by
 * single ID using JanusGraph Core API
 *
 * @author pencal
 */
@Singleton
@NdBenchClientPlugin("janusgraph-cql")
public class JanusGraphPluginCQL extends JanusGraphBasePlugin implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphPluginCQL.class);
    private static String BACKEND = "cql";

    private final JanusGraphFactory.Builder graphBuilder;

    private DataGenerator dataGenerator;
    private GraphTraversalSource traversalSource;
    private JanusGraph graph;
    private boolean useJanusgraphTransaction;

    @Inject
    public JanusGraphPluginCQL(IJanusGraphConfig config, JanusGraphBuilderCQLProvider builderProvider) {
        super(BACKEND, config.getStorageHostname(), config.getStoragePort());
        this.graphBuilder = builderProvider.getGraphBuilder();
        this.useJanusgraphTransaction = config.useJanusgraphTransaction();
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.graph = graphBuilder.open();
        this.traversalSource = graph.traversal();
        this.dataGenerator = dataGenerator;
        createSchema(graph);
        logger.info("Initing JanusGraph Plugin CQL");
    }

    @Override
    public String readSingle(String key) throws Exception {
        JanusGraphTransaction tx = useJanusgraphTransaction ? graph.newTransaction() : null;

        try {
            return readSingleInternal(key, tx);
        } finally {
            if (tx != null)
                tx.close();
        }
    }

    private String readSingleInternal(String key, JanusGraphTransaction transaction) throws Exception {
        String response = OK;
        if (useJanusgraphTransaction) {
            if (transaction == null) {
                throw new IllegalArgumentException("JanusGraph transaction in read operation is null");
            }

            JanusGraphVertex vertex = (JanusGraphVertex) transaction.query().has(PROP_CUSTOM_ID_KEY, key).vertices();

            if (vertex == null) {
                throw new Exception("Internal error when reading data with key" + key + " using JanusGraph Core API");
            }

            if (vertex.keys().isEmpty())
                response = CACHE_MISS;
        } else {
            List<Vertex> results = traversalSource.V().has(PROP_CUSTOM_ID_KEY, key).toList();

            if (results == null)
                throw new Exception("Internal error when reading data with key" + key + " using TinkerPop API");
            else if (results.size() == 0)
                response = CACHE_MISS;
        }

        return response;
    }

    @Override
    public String writeSingle(String key) throws Exception {
        if (useJanusgraphTransaction) {
                graph.addVertex(T.label, VERTEX_LABEL_LEVEL_1, PROP_CUSTOM_ID_KEY, key, PROP_METADATA_KEY,
                        dataGenerator.getRandomValue()); //Automatically opens a new transaction
                graph.tx().commit();
        } else {
            traversalSource.getGraph().addVertex(T.label, VERTEX_LABEL_LEVEL_1, PROP_CUSTOM_ID_KEY, key,
                    PROP_METADATA_KEY, dataGenerator.getRandomValue());
            traversalSource.getGraph().tx().commit();
        }

        return OK;
    }

    /**
     * Perform a bulk read operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> readBulk(final List<String> keys) throws Exception {
        List<String> responses = new ArrayList<>(keys.size());
        JanusGraphTransaction transaction = useJanusgraphTransaction ? graph.newTransaction() : null;

        try {
            for (String key : keys) {
                String response = readSingleInternal(key, transaction);
                responses.add(response);
            }
        } finally {
            if (transaction != null)
                transaction.close();
        }
        return responses;
    }

    /**
     * Perform a bulk write operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> writeBulk(final List<String> keys) throws Exception {
        List<String> responses = new ArrayList<>(keys.size());
        for (String key : keys) {
            String response = writeSingle(key);
            responses.add(response);
        }
        return responses;
    }

    @Override
    public void shutdown() throws Exception {
        graph.close();
        logger.info("JanusGraph DB shutdown");
    }

    @Override
    public String getConnectionInfo() throws Exception {
        return super.getConnectionInfo(graph);
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }
}
