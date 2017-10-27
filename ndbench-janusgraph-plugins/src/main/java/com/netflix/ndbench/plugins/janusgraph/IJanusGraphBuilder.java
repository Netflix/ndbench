package com.netflix.ndbench.plugins.janusgraph;

import org.janusgraph.core.JanusGraphFactory;

/**
 * @author pencal
 */
public interface IJanusGraphBuilder {
    JanusGraphFactory.Builder getGraphBuilder();
}
