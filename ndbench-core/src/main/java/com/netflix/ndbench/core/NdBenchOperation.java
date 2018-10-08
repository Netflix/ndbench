package com.netflix.ndbench.core;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.RateLimiter;

import com.netflix.ndbench.api.plugin.NdBenchMonitor;

public interface NdBenchOperation<K> {
        boolean process(NdBenchDriver driver,
                        NdBenchMonitor monitor,
                        List<K> keys,
                        AtomicReference<RateLimiter> rateLimiter,
                        boolean isAutoTuneEnabled);

        boolean isReadType();

        boolean isWriteType();
    }