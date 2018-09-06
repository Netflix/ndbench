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
package com.netflix.ndbench.aws.config;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Deprecated
@Configuration(prefix =  NdBenchConstants.PROP_NAMESPACE +  "aws")
public interface CredentialsConfiguration {
    /**
     * The AWS access key to use to connect to AWS services (e.g., DynamoDB). Prefer using the default credentials provider chain.
     */
    @Deprecated
    String accessKey();

    /**
     * The AWS secret key to use to connect to AWS services (e.g., DynamoDB). Prefer using the default credentials provider chain.
     */
    @Deprecated
    String secretKey();
}
