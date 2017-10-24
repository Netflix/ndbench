/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.core.config;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.archaius.api.PropertyListener;
import com.netflix.ndbench.core.NdBenchDriver;

import static com.netflix.ndbench.api.plugin.common.NdBenchConstants.PROP_NAMESPACE;

/**
 * @author vchella
 */
@Singleton
public class NdbenchConfigListener {

    @Inject
    public NdbenchConfigListener(PropertyFactory factory, NdBenchDriver ndBenchDriver)
    {
        factory.getProperty(PROP_NAMESPACE + "readRateLimit").asInteger(100).addListener(new PropertyListener<Integer>() {
            @Override
            public void onChange(Integer value) {
                ndBenchDriver.onReadRateLimitChange();
            }

            @Override
            public void onParseError(Throwable error) {

            }
        });
        factory.getProperty(PROP_NAMESPACE + "writeRateLimit").asInteger(100).addListener(new PropertyListener<Integer>() {
            @Override
            public void onChange(Integer value) {
                ndBenchDriver.onWriteRateLimitChange();
            }

            @Override
            public void onParseError(Throwable error) {

            }
        });
    }
}
