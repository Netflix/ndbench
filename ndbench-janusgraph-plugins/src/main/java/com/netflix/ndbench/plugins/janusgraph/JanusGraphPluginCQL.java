package com.netflix.ndbench.plugins.janusgraph;

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugins.janusgraph.configs.IJanusGraphConfig;
import com.netflix.ndbench.plugins.janusgraph.cql.JanusGraphBuilderCQLProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Iterator;
import java.util.List;

/***
 * JanusGraph benchmarking plugin to measure throughput of write and read by single ID using JanusGraph Core API
 *
 * @author pencal
 */
@Singleton
@NdBenchClientPlugin("janusgraph-cql")
public class JanusGraphPluginCQL extends JanusGraphBasePlugin implements NdBenchClient {
    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphPluginCQL.class);
    private static String BACKEND = "cql";

    private final JanusGraphFactory.Builder graphBuilder;

    private DataGenerator dataGenerator;
    private GraphTraversalSource traversalSource;
    private JanusGraph graph;
    private boolean useTinkerPop;

    @Inject
    public JanusGraphPluginCQL(IJanusGraphConfig config, JanusGraphBuilderCQLProvider builderProvider) {
        super(BACKEND, config.getStorageHostname(), config.getStoragePort());
        this.graphBuilder = builderProvider.getGraphBuilder();
        this.useTinkerPop = config.isUsingTinkerpop();
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.graph = graphBuilder.open();
        this.traversalSource = graph.traversal();
        this.dataGenerator = dataGenerator;
        createSchema(graph);
    }

    @Override
    public String readSingle(String key) throws Exception {
        String response = OK;
        if (useTinkerPop) {
            List<Vertex> results = traversalSource.V().has(PROP_CUSTOM_ID_KEY, key).toList();

            if (results == null)
                throw new Exception("Internal error when reading data with key" + key + " using TinkerPop API");
            else if (results.size() == 0)
                response = CACHE_MISS;
        }
        else  {
            try(JanusGraphTransaction tx = graph.newTransaction()) {
                Iterator<JanusGraphVertex> result = tx.query().has(PROP_CUSTOM_ID_KEY, key).vertices().iterator();
                if (result == null)
                    throw new Exception("Internal error when reading data with key" + key + " using JanusGraph Core API");
                else if (!result.hasNext())
                    response = CACHE_MISS;
            }
        }

        return response;
    }

    @Override
    public String writeSingle(String key) throws Exception {
        if (useTinkerPop) {
            Transaction tx = traversalSource.tx();
            tx.open();
            traversalSource.getGraph().addVertex(T.label, VERTEX_LABEL_LEVEL_1
                    , PROP_CUSTOM_ID_KEY, key
                    , PROP_METADATA_KEY, dataGenerator.getRandomValue());
            tx.commit();
        } else {
            try(JanusGraphTransaction tx = graph.newTransaction()) {
                tx.addVertex(T.label, VERTEX_LABEL_LEVEL_1
                        , PROP_CUSTOM_ID_KEY, key
                        , PROP_METADATA_KEY, dataGenerator.getRandomValue());
                tx.commit();
            }
        }

        return OK;
    }

    @Override
    public void shutdown() throws Exception {
        graph.close();
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
