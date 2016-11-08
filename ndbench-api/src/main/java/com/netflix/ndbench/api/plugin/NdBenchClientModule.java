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
//package com.netflix.ndbench.api.plugin;
//
//import com.google.inject.AbstractModule;
//import com.google.inject.multibindings.MapBinder;
//import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
//import org.slf4j.LoggerFactory;
//
//public abstract class NdBenchClientModule extends AbstractModule {
//    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(NdBenchClientModule.class);
//
//    private MapBinder<String, NdBenchClient> maps;
//
//
//    protected <T> void bindNdBenchClientPlugin(Class<? extends NdBenchClient> ndBenchClientImple) {
//        if (maps == null) {
//            maps = MapBinder.newMapBinder(binder(), String.class, NdBenchClient.class);
//        }
//
//        String name = getAnnotationValue(ndBenchClientImple);
//
//        maps.addBinding(name).to(ndBenchClientImple);
//    }
//
//    private String getAnnotationValue(Class<?> ndBenchClientImple) {
//        String name=ndBenchClientImple.getName();
//        try {
//            NdBenchClientPlugin annot = ndBenchClientImple.getAnnotation(NdBenchClientPlugin.class);
//            name = annot.value();
//            Logger.info("Installing NdBenchClientPlugin: "+ndBenchClientImple.getName()+" with Annotation: "+name);
//        }
//        catch (Exception e)
//        {
//            Logger.warn("No Annotation found for class :"+ name +", so loading default class name");
//        }
//        return name;
//    }
//
//    protected <T> void installNdBenchClientPlugin(Class<?> ndBenchClientImple) {
//        if (maps == null) {
//            maps = MapBinder.newMapBinder(binder(), String.class, NdBenchClient.class);
//        }
//
//        String name = getAnnotationValue(ndBenchClientImple);
//
//        maps.addBinding(name).to((Class<? extends NdBenchClient>) ndBenchClientImple);
//    }
//}
//
