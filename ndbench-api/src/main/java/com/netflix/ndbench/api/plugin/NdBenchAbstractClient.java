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

package com.netflix.ndbench.api.plugin;

import java.util.List;

/**
 * Defines default methods that provide a hook point for Auto-tuning, more information for which may be found
 * <a href="http://github.com/Netflix/ndbench/wiki/Configuration">here</a>.
 * <p>
 * <p>
 * This interface provides backward compatability with legacy client plug-ins  (which do not support auto-tuning)
 * since the interface {@link  com.netflix.ndbench.api.plugin.NdBenchClient} concretizes the type parameter 'W'
 * to the type that heretofore has been returned by all clients implementing the writeSingle method (namely: String.)
 *
 * @param <W> - the type of the result returned by {@link #writeSingle}
 *
 * @author vchella, pencal
 */
public interface NdBenchAbstractClient<W> {

    /**
     * Initialize the client
     *
     * @throws Exception
     */
    void init(DataGenerator dataGenerator) throws Exception;

    /**
     * Perform a single read operation
     *
     * @return
     * @throws Exception
     */
    String readSingle(final String key) throws Exception;

    /**
     * Perform a bulk read operation given the list of keys
     *
     * @return
     * @throws Exception
     */
    List<String> readBulk(final List<String> keys) throws Exception;

    /**
     * Perform a single write operation
     *
     * @return
     * @throws Exception
     */
    W writeSingle(final String key) throws Exception;

    /**
     * Perform bulk write operation given the list of keys
     *
     * @param keys
     * @return
     */
    List<W> writeBulk(final List<String> keys) throws Exception;

    /**
     * shutdown the client
     */
    void shutdown() throws Exception;

    /**
     * Get connection info
     */
    String getConnectionInfo() throws Exception;

    /**
     * Run workflow for functional testing
     *
     * @throws Exception
     */
    String runWorkFlow() throws Exception;

    /**
     * Implementations of the  autoTune[Write/Read]RateLimit methods will interpret the information provided by
     * 'event' and 'runStats', and 'currentRateLimit' in order to determine whether or not it is appropriate to auto-tune
     * read / write rate limits for the currently running benchmark.
     * <p>
     * This method will be called by the benchmark driver after every write operation (this applies
     * to the initial implementation -- subsequent releases may also allow tuning based on responses to read operations).
     * <p>
     * Note that if there are multiple read or write workers it is possible that an attempt by one thread to auto-tune
     * the rate limit to a particular level might be overwritten by the rate limit value set by another thread. But
     * such overwrites should not affect the eventual trend of the rate limit either downward or upward.
     *
     * @param currentRateLimit - the write rate limit currently in effect.
     * @param event            - conveys information related to the most recently performed read or write operation which
     *                         this method will use to decide  whether or not it is appropriate to auto-tune
     *                         write / read rate limits for the currently running benchmark. An event will typically
     *                         capture details of errors occurring in the context of  an attempt to perform
     *                         either a read or write operation.
     * @param runStats         - statistics such as average write/read latency for current benchmark run
     * @return - the new suggested rate limit -- ignored by driver if less-than-or-equal-to 0.
     */

    default Double autoTuneWriteRateLimit(Double currentRateLimit, List<W> event, NdBenchMonitor runStats) {
        return -1D;
    }

    /**
     * See documentation for {@link #autoTuneWriteRateLimit}
     */
    default double autoTuneReadRateLimit(double currentRateLimit, List<W> event, NdBenchMonitor runStats) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
