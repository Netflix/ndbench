package com.netflix.ndbench.plugin.es;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;

import javax.inject.Singleton;

public final class NfndbenchEsModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    IEsConfig getEsNfndbenchConfig(ConfigProxyFactory factory) {
        // Here we turn the config interface into an implementation that can load dynamic properties.
        return factory.newProxy(IEsConfig.class);
    }
}
