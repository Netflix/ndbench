package com.netflix.ndbench.plugins.janusgraph;

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

/***
 * JanusGraph benchmarking plugin to measure throughput of write and read by single ID
 *
 * @author pencal
 */
@Singleton
@NdBenchClientPlugin("janusgraph")
public class JanusGraphPlugin implements NdBenchClient {
    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphPlugin.class);

    private static final String OK = "ok";

    private static final String CACHE_MISS = null;
    private static final String COMPOSITE_INDEX_NAME = "idx_customId";

    private static final String PROP_CUSTOM_ID_KEY = "prop_customId";
    private static final String PROP_METADATA_KEY = "metadata";

    private static final String VERTEX_LABEL_LEVEL_1 = "level1";

    private final JanusGraphBuilderProviderProxy graphBuilderProviderProxy;
    private DataGenerator dataGenerator;
    private JanusGraph graph;

    @Inject
    public JanusGraphPlugin(JanusGraphBuilderProviderProxy graphBuilderProviderProxy) {
        this.graphBuilderProviderProxy = graphBuilderProviderProxy;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.graph = graphBuilderProviderProxy.getGraphBuilder().open();
        this.dataGenerator = dataGenerator;

        // Create schema
        JanusGraphManagement mgmt = graph.openManagement();
        if (mgmt.containsGraphIndex(COMPOSITE_INDEX_NAME)) {
            LOG.info("Graph schema is already setup ✓");
        } else {
            final PropertyKey customId = mgmt.makePropertyKey(PROP_CUSTOM_ID_KEY).dataType(String.class).make();
            JanusGraphManagement.IndexBuilder customIdIndexBuilder = mgmt.buildIndex(COMPOSITE_INDEX_NAME, Vertex.class).addKey(customId);
            customIdIndexBuilder.buildCompositeIndex();

            mgmt.makeVertexLabel(VERTEX_LABEL_LEVEL_1).make();
            mgmt.commit();
        }
    }

    @Override
    public String readSingle(String key) throws Exception {
        List<Vertex> results = graph.traversal().V().has(PROP_CUSTOM_ID_KEY, key).toList();

        if (results == null) {
            throw new Exception("Internal error when reading data with key" + key);
        } else if (results.size() == 0){
            return CACHE_MISS;
        } else {
            return OK;
        }
    }

    @Override
    public String writeSingle(String key) throws Exception {
        JanusGraphTransaction tx = graph.newTransaction();
        tx.addVertex(T.label, VERTEX_LABEL_LEVEL_1
                , PROP_CUSTOM_ID_KEY, key
                , PROP_METADATA_KEY, dataGenerator.getRandomValue());
        tx.commit();

        return OK;
    }

    @Override
    public void shutdown() throws Exception {
        graph.close();
    }

    @Override
    public String getConnectionInfo() throws Exception {
        return graph.isOpen() ? "Graph is opened" : "Graph is closed";
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }
}
