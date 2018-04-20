/*
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.ndbench.plugin.dynamodb.operations;

import java.util.function.Function;

/**
 * This class will allow generalizes data plane operations between AWS SDK v1 and v2.
 * @param <T> This is the class of the result type from which to pull consumed capacity
 * @param <I> This is the class of input into the data plane operation
 * @param <O> This is the class of output of the data plane operation, after being interpreted from T
 *
 * @author Alexander Patrikalakis
 */
public interface CapacityConsumingFunction<T, I, O> extends Function<I, O> {
    T measureConsumedCapacity(T t);
    double getAndResetConsumed();
}
