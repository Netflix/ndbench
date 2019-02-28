/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.core.discovery;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.ndbench.core.config.IConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * AWSAsgDiscovery assumes you have enough permissions to run autoscaling:DescribeAutoScalingInstances and
 * describeInstances request on AWS.
 *
 * This class also assumes that NdBench is deployed in an ASG.
 *
 * <b>Important:</b> Be sure to fill in your AWS access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows
 * users) before you try to run this sample.
 * @author vchella
 */
public class AwsAsgDiscovery implements IClusterDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(LocalClusterDiscovery.class.getName());

    IConfiguration config;
    @Inject
    public AwsAsgDiscovery(IConfiguration configuration)
    {
        this.config = configuration;
    }
    @Override
    public List<String> getApps() {
        return Arrays.asList(getCurrentAsgName());
    }

    @Override
    public List<String> getEndpoints(String appName, int defaultPort) {

        return getRacMembership().stream().map(s -> s+":"+defaultPort).collect(Collectors.toList());

    }

    public List<String> getRacMembership()
    {
         /*
         * Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
         * and save the following lines after replacing the underlined values with your own.
         *
         * [default]
         * aws_access_key_id = YOUR_ACCESS_KEY_ID
         * aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
         */

        AmazonAutoScaling client = null;
        AmazonEC2 ec2Client = null;

        try
        {
            client = getAutoScalingClient();
            ec2Client = AmazonEC2ClientBuilder.standard().build();

            String myAsgName = getCurrentAsgName();

            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(myAsgName);

            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                for (com.amazonaws.services.autoscaling.model.Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.getInstanceId());
            }
            logger.info(String.format("Querying Amazon returned following instance in the ASG: %s --> %s", myAsgName, StringUtils.join(instanceIds, ",")));


            DescribeInstancesRequest insReq = new DescribeInstancesRequest().withInstanceIds(instanceIds);

            DescribeInstancesResult insRes = ec2Client.describeInstances(insReq);

            return insRes.getReservations().stream()
                    .flatMap(r -> r.getInstances().stream())
                    .map(Instance::getPublicDnsName).distinct().collect(Collectors.toList());
        }
        catch (Exception e)
        {
            logger.error("Exception in getting private IPs from current ASG",e);
            return Collections.emptyList();
        }
        finally
        {
            if (client != null)
                client.shutdown();
            if(ec2Client !=null)
                ec2Client.shutdown();
        }
    }

    private String getCurrentAsgName()
    {
        DescribeAutoScalingInstancesRequest asgInsReq = new DescribeAutoScalingInstancesRequest()
                .withInstanceIds(AWSUtil.getLocalInstanceId());

        DescribeAutoScalingInstancesResult asgInsRes = getAutoScalingClient().describeAutoScalingInstances(asgInsReq);
        String myAsgName = asgInsRes.getAutoScalingInstances().get(0).getAutoScalingGroupName();
        return myAsgName!=null && myAsgName.length() > 0 ? myAsgName : "NdBench_Aws_cluster";
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        AmazonAutoScaling client = AmazonAutoScalingClientBuilder.standard().build();
        return client;
    }


}
