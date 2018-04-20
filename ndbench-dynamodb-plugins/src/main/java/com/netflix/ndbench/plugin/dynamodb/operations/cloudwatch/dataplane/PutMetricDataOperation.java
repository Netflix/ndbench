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
package com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.dataplane;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.AbstractCloudWatchOperation;

import java.util.function.Function;

/**
 * @author Alexander Patrikalakis
 */
public class PutMetricDataOperation extends AbstractCloudWatchOperation implements Function<PutMetricDataRequest, PutMetricDataResult> {
    public PutMetricDataOperation(AmazonCloudWatch cloudWatch) {
        super(cloudWatch);
    }

    @Override
    public PutMetricDataResult apply(PutMetricDataRequest putMetricDataRequest) {
        try {
            return cloudWatch.putMetricData(putMetricDataRequest);
        } catch (AmazonServiceException ase) {
            throw amazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw amazonClientException(ace);
        }
    }
}
