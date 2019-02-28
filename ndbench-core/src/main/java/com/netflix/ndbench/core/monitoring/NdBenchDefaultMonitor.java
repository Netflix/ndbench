/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.core.monitoring;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.core.config.IConfiguration;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author vchella
 */
@Singleton
public class NdBenchDefaultMonitor implements NdBenchMonitor
{

    private final IConfiguration config;
    private final MetricRegistry metrics;

    private final Histogram readHistogram;
    private final Histogram writeHistogram;

    private final Counter readSuccess;
    private final Counter readFailure;
    private final Counter writeSuccess;
    private final Counter writeFailure;
    private final Meter cacheHits;
    private final Meter cacheMiss;
    private final Meter readRPS;
    private final Meter writeRPS;


    @Inject
    public NdBenchDefaultMonitor(IConfiguration config)
    {
        this.config = config;
        this.metrics = new MetricRegistry();

        readHistogram = metrics.histogram(name("NdBenchDefaultMonitor", "readlatency"));
        writeHistogram = metrics.histogram(name("NdBenchDefaultMonitor", "writelatency"));
        readSuccess = metrics.counter(name("NdBenchDefaultMonitor", "readSuccess"));
        readFailure = metrics.counter(name("NdBenchDefaultMonitor", "readFailure"));
        writeSuccess = metrics.counter(name("NdBenchDefaultMonitor", "writeSuccess"));
        writeFailure = metrics.counter(name("NdBenchDefaultMonitor", "writeFailure"));
        cacheHits = metrics.meter(name("NdBenchDefaultMonitor", "cacheHits"));
        cacheMiss = metrics.meter(name("NdBenchDefaultMonitor", "cacheMiss"));
        readRPS = metrics.meter(name("NdBenchDefaultMonitor", "readRPS"));
        writeRPS = metrics.meter(name("NdBenchDefaultMonitor", "writeRPS"));

        //Starting JMXReporter
        final JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("netflix.ndbench.metrics").build();
        reporter.start();
    }

    @Override
    public void initialize()
    {

    }

    @Override
    public void incReadSuccess() {
        readSuccess.inc();
        readRPS.mark();
    }

    @Override
    public long getReadSuccess() {
        return readSuccess.getCount();
    }

    @Override
    public void incReadFailure() {
        readFailure.inc();
        readRPS.mark();
    }

    @Override
    public long getReadFailure() {
        return readFailure.getCount();
    }

    @Override
    public void incWriteSuccess() {
        writeSuccess.inc();
        writeRPS.mark();
    }

    @Override
    public long getWriteSuccess() {
        return writeSuccess.getCount();
    }

    @Override
    public void incWriteFailure() {
        writeFailure.inc();
        writeRPS.mark();
    }

    @Override
    public long getWriteFailure() {
        return writeFailure.getCount();
    }

    @Override
    public void incCacheHit() {
        cacheHits.mark();
    }

    @Override
    public long getCacheHits() {
        return cacheHits.getCount();
    }

    @Override
    public void incCacheMiss() {
        cacheMiss.mark();
    }

    @Override
    public long getCacheMiss() {
        return cacheMiss.getCount();
    }

    @Override
    public void recordReadLatency(long duration) {
        readHistogram.update(duration);
    }

    @Override
    public long getReadLatAvg() {
        return longValueOfDouble(readHistogram.getSnapshot().getMean());
    }

    @Override
    public long getReadLatP50() {
        return longValueOfDouble(readHistogram.getSnapshot().getMedian());
    }

    @Override
    public long getReadLatP95() {
        return longValueOfDouble(readHistogram.getSnapshot().get95thPercentile());
    }

    @Override
    public long getReadLatP99() {
        return longValueOfDouble(readHistogram.getSnapshot().get99thPercentile());
    }

    @Override
    public long getReadLatP995() {
        return longValueOfDouble(readHistogram.getSnapshot().getValue(.995));
    }

    @Override
    public long getReadLatP999() {
        return longValueOfDouble(readHistogram.getSnapshot().get999thPercentile());
    }

    @Override
    public long getWriteLatAvg() {
        return longValueOfDouble(writeHistogram.getSnapshot().getMean());
    }

    @Override
    public long getWriteLatP50() {
        return longValueOfDouble(writeHistogram.getSnapshot().getMedian());
    }

    @Override
    public long getWriteLatP95() {
        return longValueOfDouble(writeHistogram.getSnapshot().get95thPercentile());
    }

    @Override
    public long getWriteLatP99() {
        return longValueOfDouble(writeHistogram.getSnapshot().get99thPercentile());
    }

    @Override
    public long getWriteLatP995() {
        return longValueOfDouble(writeHistogram.getSnapshot().getValue(0.995));
    }

    @Override
    public long getWriteLatP999() {
        return longValueOfDouble(writeHistogram.getSnapshot().get999thPercentile());
    }

    @Override
    public long getWriteRPS() {
        return longValueOfDouble(writeRPS.getOneMinuteRate());
    }

    @Override
    public long getReadRPS() {
        return longValueOfDouble(readRPS.getOneMinuteRate());
    }

    @Override
    public void setWriteRPS(long writeRPS) {
        // setting RPS does not apply here since, we are tracking RPS via writeSuccess and writeFailure calls
    }

    @Override
    public void setReadRPS(long readRPS) {
        // setting RPS does not apply here since, we are tracking RPS via readSuccess and readFailure calls
    }

    @Override
    public void recordWriteLatency(long duration) {
        writeHistogram.update(duration);
    }

    @Override
    public int getCacheHitRatioInt() {
        return (int) getCacheHitRatio();
    }

    @Override
    public void resetStats() {
    }

    private float getCacheHitRatio() {
        long hits = cacheHits.getCount();
        long miss = cacheMiss.getCount();

        if (hits + miss == 0) {
            return 0;
        }

        return (float) (hits * 100L) / (float) (hits + miss);
    }

    private long longValueOfDouble(double d) {
        return Double.valueOf(d).longValue();
    }

}
