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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import software.amazon.awssdk.core.auth.AwsCredentialsProvider;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.model.DescribeScalableTargetsRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.DescribeScalingPoliciesRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.MetricType;
import software.amazon.awssdk.services.applicationautoscaling.model.PolicyType;
import software.amazon.awssdk.services.applicationautoscaling.model.PredefinedMetricSpecification;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.ScalableDimension;
import software.amazon.awssdk.services.applicationautoscaling.model.ServiceNamespace;
import software.amazon.awssdk.services.applicationautoscaling.model.TargetTrackingScalingPolicyConfiguration;
import software.amazon.awssdk.services.dynamodb.model.DescribeLimitsResponse;
import software.amazon.awssdk.services.sts.STSClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;

@Singleton
public class DynamoDBAutoscalingConfigurer {
    private static final int DEFAULT_SCALE_IN_OUT_COOLDOWN = 60;

    private final ApplicationAutoScalingClient autoScalingClient;
    private final STSClient securityTokenService;

    @Inject
    public DynamoDBAutoscalingConfigurer(final AwsCredentialsProvider awsCredentialsProvider) {
        autoScalingClient = ApplicationAutoScalingClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .build();
        securityTokenService = STSClient.builder()
                .credentialsProvider(awsCredentialsProvider)
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
                                  DescribeLimitsResponse limits,
                                  Integer targetWriteUtilization,
                                  Integer targetReadUtilization) {
        String resourceID = "table/" + tableName;
        String accountId = securityTokenService.getCallerIdentity(GetCallerIdentityRequest.builder().build()).account();
        String serviceRoleArn = "arn:aws:iam::" + accountId
                + ":role/aws-service-role/dynamodb.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_"
                + tableName;
        createAndVerifyScalableTargetAndScalingPolicy(serviceRoleArn,
                resourceID,
                ScalableDimension.DYNAMODB_TABLE_WRITE_CAPACITY_UNITS,
                MetricType.DYNAMO_DB_WRITE_CAPACITY_UTILIZATION,
                writeCapacityUnits,
                limits.tableMaxWriteCapacityUnits().intValue(),
                targetWriteUtilization);
        createAndVerifyScalableTargetAndScalingPolicy(serviceRoleArn,
                resourceID,
                ScalableDimension.DYNAMODB_TABLE_READ_CAPACITY_UNITS,
                MetricType.DYNAMO_DB_READ_CAPACITY_UTILIZATION,
                readCapacityUnits,
                limits.tableMaxReadCapacityUnits().intValue(),
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
                TargetTrackingScalingPolicyConfiguration.builder()
                        .predefinedMetricSpecification(
                                PredefinedMetricSpecification.builder().predefinedMetricType(metricType).build())
                        .targetValue(targetValue)
                        .scaleInCooldown(DEFAULT_SCALE_IN_OUT_COOLDOWN)
                        .scaleOutCooldown(DEFAULT_SCALE_IN_OUT_COOLDOWN)
                        .build();

        // Create the scaling policy, based on your configuration
        PutScalingPolicyRequest pspRequest = PutScalingPolicyRequest.builder()
                .serviceNamespace(ServiceNamespace.DYNAMODB)
                .scalableDimension(scalableDimension)
                .resourceId(resourceID)
                .policyName("My" + scalableDimension.name() + "ScalingPolicy")
                .policyType(PolicyType.TARGET_TRACKING_SCALING)
                .targetTrackingScalingPolicyConfiguration(targetTrackingScalingPolicyConfiguration)
                .build();

        final String scalingPolicyArn;
        try {
            scalingPolicyArn = autoScalingClient.putScalingPolicy(pspRequest).policyARN();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to put scaling policy", e);
        }

        // Verify that the scaling policy was created
        DescribeScalingPoliciesRequest dspRequest = DescribeScalingPoliciesRequest.builder()
                .serviceNamespace(ServiceNamespace.DYNAMODB)
                .scalableDimension(scalableDimension)
                .resourceId(resourceID)
                .build();

        try {
            autoScalingClient.describeScalingPolicies(dspRequest).scalingPolicies().stream()
                    .filter(scalingPolicy -> scalingPolicy.policyARN().equals(scalingPolicyArn))
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
        RegisterScalableTargetRequest rstRequest = RegisterScalableTargetRequest.builder()
                .serviceNamespace(ServiceNamespace.DYNAMODB)
                .resourceId(resourceID)
                .scalableDimension(scalableDimension)
                .minCapacity(minimumCapacity)
                .maxCapacity(maximumCapacity)
                .roleARN(serviceRoleArn)
                .build();

        try {
            autoScalingClient.registerScalableTarget(rstRequest);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to register scalable target", e);
        }

        // Verify that the target was created
        // note that scalable targets do not have arns of their own so need to verify the contents
        DescribeScalableTargetsRequest dscRequest = DescribeScalableTargetsRequest.builder()
                .serviceNamespace(ServiceNamespace.DYNAMODB)
                .scalableDimension(scalableDimension)
                .resourceIds(resourceID)
                .build();
        try {
            autoScalingClient.describeScalableTargets(dscRequest).scalableTargets().stream()
                    .filter(target -> target.resourceId().equals(resourceID)
                            && target.scalableDimension().equals(scalableDimension.toString())
                            && target.serviceNamespace().equals(ServiceNamespace.DYNAMODB.toString())
                            && target.minCapacity().equals(minimumCapacity)
                            && target.maxCapacity().equals(maximumCapacity)
                            && target.roleARN().equals(serviceRoleArn))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Target was created but not all fields matched"));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to verify scalable target", e);
        }
    }
}
