/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.core.discovery;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.ndbench.core.config.IConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.ec2.model.Instance;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author vchella
 */
public class AWSAsgDiscovery  implements IClusterDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(LocalClusterDiscovery.class.getName());

    IConfiguration config;
    @Inject
    public AWSAsgDiscovery(IConfiguration configuration)
    {
        this.config = configuration;
    }
    @Override
    public List<String> getApps() {
        return Arrays.asList("TESTING");
    }

    @Override
    public List<String> getEndpoints(String appName, int defaultPort) {

        return getRacMembership();

    }

    public List<String> getRacMembership()
    {
        List<String> instanceIps = new LinkedList<>();
        AmazonAutoScaling client = null;
        AmazonEC2 ec2Client = null;
        try
        {
            client = AmazonAutoScalingClientBuilder.standard().build();
            ec2Client = AmazonEC2ClientBuilder.standard().build();

            DescribeAutoScalingInstancesRequest asgInsReq = new DescribeAutoScalingInstancesRequest()
                    .withInstanceIds(AWSUtil.getLocalInstanceId());

            DescribeAutoScalingInstancesResult asgInsRes = client.describeAutoScalingInstances(asgInsReq);
            String myAsgName = asgInsRes.getAutoScalingInstances().get(0).getAutoScalingGroupName();



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

            instanceIps =  insRes.getReservations().get(0)
                    .getInstances()
                    .stream()
                    .map(Instance::getPrivateIpAddress)
                    .collect(Collectors.toList());



            return instanceIps;
        }
        catch (Exception e)
        {
            logger.error("Exception in getting private IPs",e);
        }
        finally
        {
            if (client != null)
                client.shutdown();
            if(ec2Client !=null)
                ec2Client.shutdown();
        }
        return instanceIps;
    }

}
