package com.netflix.ndbench.plugin.es;

/**
 * Created by cbedford on 7/10/17.
 */
public class WriteResult {
    public static final WriteResult TOTALLY_FINE_RESULT = new WriteResult(0);

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
