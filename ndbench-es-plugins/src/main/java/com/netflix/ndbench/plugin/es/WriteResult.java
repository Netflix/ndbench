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
package com.netflix.ndbench.plugin.es;

import org.elasticsearch.client.RestClient;

public class WriteResult {
    /**
     * This constant is provided as a place holder (constant) return value  for
     * {@link EsWriter#writeDocument(RestClient, String, Boolean)} so that for now we effectively ignore
     * numRejectedExecutionExceptions -- artificially considering them to be zero.  At some future time
     * these exceptions should affect what write rate we recommend during auto-tuning (i.e., if we see these
     * exceptions occuring we might want to back off on the rate.)
     */
    public static final WriteResult PROVISIONAL_RESULT_THAT_ASSUMES_ALL_WENT_WELL = new WriteResult(0);

    public final Integer numRejectedExecutionExceptions;

    public WriteResult(Integer numRejectedExecutionExceptions) {
        this.numRejectedExecutionExceptions = numRejectedExecutionExceptions;
    }

    @Override
    public String toString() {
        return "WriteResult{" +
                "numRejectedExecutionExceptions=" + numRejectedExecutionExceptions +
                '}';
    }
}
