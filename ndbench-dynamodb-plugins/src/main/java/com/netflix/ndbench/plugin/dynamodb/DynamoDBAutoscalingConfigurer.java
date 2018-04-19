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
package com.netflix.ndbench.plugin.dynamodb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScaling;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScalingClientBuilder;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsRequest;
import com.amazonaws.services.applicationautoscaling.model.DescribeScalingPoliciesRequest;
import com.amazonaws.services.applicationautoscaling.model.MetricType;
import com.amazonaws.services.applicationautoscaling.model.PolicyType;
import com.amazonaws.services.applicationautoscaling.model.PredefinedMetricSpecification;
import com.amazonaws.services.applicationautoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import com.amazonaws.services.applicationautoscaling.model.ScalableDimension;
import com.amazonaws.services.applicationautoscaling.model.ServiceNamespace;
import com.amazonaws.services.applicationautoscaling.model.TargetTrackingScalingPolicyConfiguration;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class DynamoDBAutoscalingConfigurer {
    private static final int DEFAULT_SCALE_IN_OUT_COOLDOWN = 60;

    private final AWSApplicationAutoScaling autoScalingClient;
    private final AWSSecurityTokenService securityTokenService;

    @Inject
    public DynamoDBAutoscalingConfigurer(final AWSCredentialsProvider awsCredentialsProvider) {
        autoScalingClient = AWSApplicationAutoScalingClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .build();
        securityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .build();
    }

    /**
     * configure autoscaling as per aws docs:
     * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.HowTo.SDK.html
     * @param readCapacityUnits
     * @param writeCapacityUnits
     * @param tableName
     * @param limits
     * @param targetWriteUtilization
     * @param targetReadUtilization
     */
    public void setupAutoscaling(Long readCapacityUnits,
                                  Long writeCapacityUnits,
                                  String tableName,
                                  DescribeLimitsResult limits,
                                  Integer targetWriteUtilization,
                                  Integer targetReadUtilization) {
        String resourceID = "table/" + tableName;
        String accountId = securityTokenService.getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
        String serviceRoleArn = "arn:aws:iam::" + accountId
                + ":role/aws-service-role/dynamodb.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_"
                + tableName;
        createAndVerifyScalableTargetAndScalingPolicy(serviceRoleArn,
                resourceID,
                ScalableDimension.DynamodbTableWriteCapacityUnits,
                MetricType.DynamoDBWriteCapacityUtilization,
                writeCapacityUnits,
                limits.getTableMaxWriteCapacityUnits().intValue(),
                targetWriteUtilization);
        createAndVerifyScalableTargetAndScalingPolicy(serviceRoleArn,
                resourceID,
                ScalableDimension.DynamodbTableReadCapacityUnits,
                MetricType.DynamoDBReadCapacityUtilization,
                readCapacityUnits,
                limits.getTableMaxReadCapacityUnits().intValue(),
                targetReadUtilization);
    }

    private void createAndVerifyScalableTargetAndScalingPolicy(String serviceRoleArn,
                                                               String resourceID,
                                                               ScalableDimension scalableDimension,
                                                               MetricType metricType,
                                                               Long capacityUnits,
                                                               Integer maxCapacityUnits,
                                                               Integer utilizationTarget) {
        createAndVerifyScalableTarget(resourceID, serviceRoleArn, scalableDimension, capacityUnits.intValue(),
                maxCapacityUnits);
        createAndVerifyScalingPolicy(resourceID, scalableDimension, metricType, utilizationTarget.doubleValue());
    }

    private void createAndVerifyScalingPolicy(final String resourceID,
                                              final ScalableDimension scalableDimension,
                                              final MetricType metricType,
                                              final Double targetValue) {
        // Configure a scaling policy
        TargetTrackingScalingPolicyConfiguration targetTrackingScalingPolicyConfiguration =
                new TargetTrackingScalingPolicyConfiguration()
                        .withPredefinedMetricSpecification(new PredefinedMetricSpecification().withPredefinedMetricType(metricType))
                        .withTargetValue(targetValue)
                        .withScaleInCooldown(DEFAULT_SCALE_IN_OUT_COOLDOWN)
                        .withScaleOutCooldown(DEFAULT_SCALE_IN_OUT_COOLDOWN);

        // Create the scaling policy, based on your configuration
        PutScalingPolicyRequest pspRequest = new PutScalingPolicyRequest()
                .withServiceNamespace(ServiceNamespace.Dynamodb)
                .withScalableDimension(scalableDimension)
                .withResourceId(resourceID)
                .withPolicyName("My" + scalableDimension.name() + "ScalingPolicy")
                .withPolicyType(PolicyType.TargetTrackingScaling)
                .withTargetTrackingScalingPolicyConfiguration(targetTrackingScalingPolicyConfiguration);

        final String scalingPolicyArn;
        try {
            scalingPolicyArn = autoScalingClient.putScalingPolicy(pspRequest).getPolicyARN();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to put scaling policy", e);
        }

        // Verify that the scaling policy was created
        DescribeScalingPoliciesRequest dspRequest = new DescribeScalingPoliciesRequest()
                .withServiceNamespace(ServiceNamespace.Dynamodb)
                .withScalableDimension(scalableDimension)
                .withResourceId(resourceID);

        try {
            autoScalingClient.describeScalingPolicies(dspRequest).getScalingPolicies().stream()
                    .filter(scalingPolicy -> scalingPolicy.getPolicyARN().equals(scalingPolicyArn))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Scaling policy was created but arn did not match"));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to verify scaling policy", e);
        }
    }

    private void createAndVerifyScalableTarget(final String resourceID,
                                               final String serviceRoleArn,
                                               final ScalableDimension scalableDimension,
                                               final Integer minimumCapacity,
                                               final Integer maximumCapacity) {
        // Define the scalable targets
        RegisterScalableTargetRequest rstRequest = new RegisterScalableTargetRequest()
                .withServiceNamespace(ServiceNamespace.Dynamodb)
                .withResourceId(resourceID)
                .withScalableDimension(scalableDimension)
                .withMinCapacity(minimumCapacity)
                .withMaxCapacity(maximumCapacity)
                .withRoleARN(serviceRoleArn);

        try {
            autoScalingClient.registerScalableTarget(rstRequest);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to register scalable target", e);
        }

        // Verify that the target was created
        // note that scalable targets do not have arns of their own so need to verify the contents
        DescribeScalableTargetsRequest dscRequest = new DescribeScalableTargetsRequest()
                .withServiceNamespace(ServiceNamespace.Dynamodb)
                .withScalableDimension(scalableDimension)
                .withResourceIds(resourceID);
        try {
            autoScalingClient.describeScalableTargets(dscRequest).getScalableTargets().stream()
                    .filter(target -> target.getResourceId().equals(resourceID)
                            && target.getScalableDimension().equals(scalableDimension.toString())
                            && target.getServiceNamespace().equals(ServiceNamespace.Dynamodb.toString())
                            && target.getMinCapacity().equals(minimumCapacity)
                            && target.getMaxCapacity().equals(maximumCapacity)
                            && target.getRoleARN().equals(serviceRoleArn))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Target was created but not all fields matched"));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to verify scalable target", e);
        }
    }
}
