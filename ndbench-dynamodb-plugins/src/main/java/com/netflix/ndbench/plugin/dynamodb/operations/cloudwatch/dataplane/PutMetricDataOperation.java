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

import com.netflix.ndbench.plugin.dynamodb.operations.cloudwatch.AbstractCloudWatchOperation;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.util.function.Function;

/**
 * @author Alexander Patrikalakis
 */
public class PutMetricDataOperation extends AbstractCloudWatchOperation implements Function<PutMetricDataRequest, PutMetricDataResponse> {
    public PutMetricDataOperation(CloudWatchClient cloudWatch) {
        super(cloudWatch);
    }

    @Override
    public PutMetricDataResponse apply(PutMetricDataRequest putMetricDataRequest) {
        try {
            return cloudWatch.putMetricData(putMetricDataRequest);
        } catch (SdkServiceException ase) {
            throw sdkServiceException(ase);
        } catch (SdkClientException ace) {
            throw sdkClientException(ace);
        }
    }
}
