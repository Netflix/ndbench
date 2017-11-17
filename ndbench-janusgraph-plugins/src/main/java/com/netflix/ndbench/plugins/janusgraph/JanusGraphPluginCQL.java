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
 * JanusGraph benchmarking plugin to measure throughput of write and read by
 * single ID using JanusGraph Core API
 *
 * @author pencal
 */
@Singleton
@NdBenchClientPlugin("janusgraph-cql")
public class JanusGraphPluginCQL extends JanusGraphBasePlugin implements NdBenchClient {
    private static final Logger Logger = LoggerFactory.getLogger(JanusGraphPluginCQL.class);
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
    }

    @Override
    public String readSingle(String key) throws Exception {
        String response = OK;
        if (useJanusgraphTransaction) {
            try (JanusGraphTransaction tx = graph.newTransaction()) {
                JanusGraphVertex vertex = (JanusGraphVertex) tx.query().has(PROP_CUSTOM_ID_KEY, key).vertices();              
                if (vertex == null) {
                    tx.commit();
                    throw new Exception(
                            "Internal error when reading data with key" + key + " using JanusGraph Core API");
                }
                if(vertex.keys().isEmpty())
                    response = CACHE_MISS;
                
                tx.commit();
            } 

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
