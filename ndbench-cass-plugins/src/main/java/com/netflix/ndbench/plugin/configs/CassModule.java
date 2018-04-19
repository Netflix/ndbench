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
package com.netflix.ndbench.plugin.configs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;

@NdBenchClientPluginGuiceModule
public class CassModule extends AbstractModule {
    @Override
    protected void configure() {
        //NO-OP
    }

    @Provides
    CassandraAstynaxConfiguration getAstynaxConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CassandraAstynaxConfiguration.class);
    }

    @Provides
    CassandraGenericConfiguration getCassandraGenericConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CassandraGenericConfiguration.class);
    }

    @Provides
    CassandraUdtConfiguration getCassandraUdtConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CassandraUdtConfiguration.class);
    }

    @Provides
    ElassandraConfiguration getElassandraConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ElassandraConfiguration.class);
    }
}
