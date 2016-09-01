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
