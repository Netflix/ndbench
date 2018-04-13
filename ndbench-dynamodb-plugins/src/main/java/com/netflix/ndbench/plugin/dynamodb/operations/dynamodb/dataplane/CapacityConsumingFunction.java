package com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane;

import java.util.function.Function;

public interface CapacityConsumingFunction<T, I, O> extends Function<I, O> {
    T measureConsumedCapacity(T t);
}
