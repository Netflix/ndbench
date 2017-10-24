package com.netflix.ndbench.core.config;

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.ndbench.core.defaultimpl.NdBenchClientModule;
import com.netflix.ndbench.core.defaultimpl.NdBenchGuiceModule;

import java.util.Arrays;
import java.util.List;

/**
 * This is a provisional implementation of this class. The next commit will hook in google reflections-based
 * discovery of all modules to be added.
 */
public class GuiceInjectorProvider {
    private List<Module> getModuleList(AbstractModule... modules) {
        List<Module> moduleList = Lists.newArrayList();

        moduleList.add(new NdBenchGuiceModule());
        moduleList.add(new NdBenchClientModule());
        moduleList.add(new ArchaiusModule()); //Archaius-2

        moduleList.addAll(Arrays.asList(modules));
        return moduleList;
    }

    /**
     * Creates an injector using modules obtained from the following sources:  (1) the private getModulesList method
     * of this class,  (2)  the  'modules' list passed as the first and only argument to this method, and (3) all
     * modules that are auto-discovered via reflection as a result of advertising themselves via the annotation
     * {@link com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule}
     *
     * @param modules - any additional guice binding modules to add to those added by default
     */
    public Injector getInjector(AbstractModule ... modules) {
        List<Module> moduleList = getModuleList(modules);
        Injector injector = Guice.createInjector(moduleList);
        injector.getInstance(IConfiguration.class).initialize();
        return injector;
    }
}
