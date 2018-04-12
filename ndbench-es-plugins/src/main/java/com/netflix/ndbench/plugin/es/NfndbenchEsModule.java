/*
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.ndbench.plugin.es;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;

import javax.inject.Singleton;


@NdBenchClientPluginGuiceModule
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
