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
package com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.controlplane;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmResult;
import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.AbstractCloudWatchOperation;

import java.util.function.Function;

/**
 * @author Alexander Patrikalakis
 */
public class PutMetricAlarmOperation extends AbstractCloudWatchOperation implements Function<PutMetricAlarmRequest, PutMetricAlarmResult> {
    public PutMetricAlarmOperation(AmazonCloudWatch cloudWatch) {
        super(cloudWatch);
    }

    @Override
    public PutMetricAlarmResult apply(PutMetricAlarmRequest putMetricAlarmRequest) {
        try {
            return cloudWatch.putMetricAlarm(putMetricAlarmRequest);
        } catch (AmazonServiceException ase) {
            throw amazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw amazonClientException(ace);
        }
    }
}
