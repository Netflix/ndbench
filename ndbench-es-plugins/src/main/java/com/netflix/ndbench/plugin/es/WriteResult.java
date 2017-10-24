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
