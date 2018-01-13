package com.netflix.ndbench.plugins.janusgraph;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;
import com.netflix.ndbench.plugins.janusgraph.configs.IJanusGraphConfig;
import com.netflix.ndbench.plugins.janusgraph.configs.cql.ICQLConfig;

/**
 * @author pencal
 */
@NdBenchClientPluginGuiceModule
public class NdBenchJanusGraphModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    IJanusGraphConfig getJanusGraphConfig(ConfigProxyFactory factory) {
        return factory.newProxy(IJanusGraphConfig.class);
    }

    @Provides
    ICQLConfig getCQLConfig(ConfigProxyFactory factory) {
        return factory.newProxy(ICQLConfig.class);
    }
}
