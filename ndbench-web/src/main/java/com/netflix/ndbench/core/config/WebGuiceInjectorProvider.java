package com.netflix.ndbench.core.config;

import com.google.inject.*;
import com.google.inject.util.Modules;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.providers.MyDataCenterInstanceConfigProvider;
import com.netflix.discovery.guice.EurekaModule;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * ndbench-web app-specific GuiceInjectorProvider impl which adds EurekaModule (local or AWS) to avoid conflicts from
 * different modules instantiating different LookupService implementations.
 * <p>
 * {@inheritDoc}
 */
public class WebGuiceInjectorProvider extends GuiceInjectorProvider {
    /**
     * GuiceInjectorProvider impl which adds EurekaModule (local or AWS) to avoid conflicts from different modules
     * instantiating different LookupService implementations.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Injector getInjector(AbstractModule... modules) {
        List<Module> moduleList = getModuleList(modules);

        // Add EurekaModule for any plugins which require it.
        // Currently needed only for ndbench-evcache-plugins, but we don't add EurekaModule to module-level
        // because choice of LookupService impl should be made at application level to avoid conflicts from
        // different modules creating different LookupService implementations.
        String discoveryEnv = System.getenv(NdBenchConstants.DISCOVERY_ENV);
        if(StringUtils.isBlank(discoveryEnv) || discoveryEnv == "local"){
            moduleList.add(Modules.override(new EurekaModule()).with(new AbstractModule() {
                @Override
                protected void configure() {
                    // Default EurekaInstanceConfig is CloudInstanceConfig, which works only in AWS env.
                    // When not in AWS, override to use MyDataCenterInstanceConfig instead.
                    bind(EurekaInstanceConfig.class).toProvider(MyDataCenterInstanceConfigProvider.class).in(Scopes.SINGLETON);
                }
            }));
        } else {
            moduleList.add(new EurekaModule());
        }

        Injector injector = Guice.createInjector(moduleList);
        injector.getInstance(IConfiguration.class).initialize();
        return injector;
    }
}