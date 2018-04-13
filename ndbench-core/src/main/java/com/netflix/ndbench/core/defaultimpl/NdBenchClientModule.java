/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.ndbench.core.defaultimpl;


import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Uses reflection to discover all NdBench client plugins which (a) reside within the package namespace
 * "com.netflix.ndbench", and (b) are annotated with {@link com.netflix.ndbench.core.defaultimpl.NdBenchClientModule}.
 * The implementing class of each thusly discovered client plugin and the plugin's name (extracted as the
 * parameter to each annotation) are used as entries in a map that enables the plugin's class to be looked up by name.
 * <p>
 * This class uses similar reflection-based discovery to find all Guice modules required by client plugins.
 * Any plugin client which needs Guice bindings only needs to annotate its Guice module with
 * {@link com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule}, and that module will be
 * auto-installed by this class.
 */
public class NdBenchClientModule extends AbstractModule {
    private static final Logger logger = LoggerFactory.getLogger(NdBenchClientModule.class);

    private MapBinder<String, NdBenchAbstractClient<?>> maps;

    private String getAnnotationValue(Class<?> ndBenchClientImpl) {
        String name = ndBenchClientImpl.getName();
        try {
            NdBenchClientPlugin annot = ndBenchClientImpl.getAnnotation(NdBenchClientPlugin.class);
            name = annot.value();
            logger.info("Installing NdBenchClientPlugin: " + ndBenchClientImpl.getName() + " with Annotation: " + name);
        } catch (Exception e) {
            logger.warn("No Annotation found for class :" + name + ", so loading default class name");
        }
        return name;
    }

    private <T> void installNdBenchClientPlugin(Class<?> ndBenchClientImpl) {
        if (maps == null) {
            TypeLiteral<String> stringTypeLiteral = new TypeLiteral<String>() {
            };
            TypeLiteral<NdBenchAbstractClient<?>> ndbClientTypeLiteral = (new TypeLiteral<NdBenchAbstractClient<?>>() {
            });
            maps = MapBinder.newMapBinder(binder(), stringTypeLiteral, ndbClientTypeLiteral);
        }

        String name = getAnnotationValue(ndBenchClientImpl);


        maps.addBinding(name).to((Class<? extends NdBenchAbstractClient<?>>) ndBenchClientImpl);
    }

    @Override
    protected void configure() {
        //Get all implementations of NdBenchClient Interface and install them as Plugins
        Reflections reflections = new Reflections("com.netflix.ndbench.plugin");
        final Set<Class<?>> classes = reflections.getTypesAnnotatedWith(NdBenchClientPlugin.class);
        for (Class<?> ndb : classes) {
            installNdBenchClientPlugin(ndb);
        }
        installGuiceBindingsRequiredByClientPlugins();
    }

    private void installGuiceBindingsRequiredByClientPlugins() {
        // Discover guice binding modules for ndbench client plugins, and add them to list
        Reflections reflections = new Reflections("com.netflix.ndbench.plugin");
        final Set<Class<?>> classes = reflections.getTypesAnnotatedWith(NdBenchClientPluginGuiceModule.class);
        for (Class<?> ndb : classes) {
            AbstractModule e = instantiateGuiceModule(ndb);
            install(e);
        }
    }


    private AbstractModule instantiateGuiceModule(Class moduleClass) {
        logger.info("adding ndbench client plugin guice module: {}", moduleClass.getCanonicalName());
        Object object;
        try {
            object = moduleClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(
                    "Failed to invoke no argument constructor of Guice binding module class " +
                            moduleClass.getCanonicalName());
        }
        return (AbstractModule) object;
    }
}

