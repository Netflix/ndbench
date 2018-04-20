package com.netflix.ndbench.plugin.dynamodb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.archaius.test.TestPropertyOverride;
import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import com.netflix.ndbench.aws.defaultimpl.AwsDefaultsModule;
import com.netflix.ndbench.core.defaultimpl.NdBenchGuiceModule;
import com.netflix.ndbench.core.generators.DefaultDataGenerator;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBModule;
import com.netflix.ndbench.plugin.dynamodb.configs.ProgrammaticDynamoDBConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({DynamoDBModule.class, ArchaiusModule.class, NdBenchGuiceModule.class, AwsDefaultsModule.class})
public class DynamoDBProgrammaticKeyValueTest {
    @Inject
    ProgrammaticDynamoDBConfiguration configuration;
    @Inject
    DefaultDataGenerator dataGenerator;

    AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("a", "b"));

    DynamoDBAutoscalingConfigurer dynamoDBAutoscalingConfigurer = mock(DynamoDBAutoscalingConfigurer.class);
    DynamoDBProxyServer server;

    @Before
    public void setup() throws Exception {
        server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", "4567"});
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testCreateDynamoDbFromDataGenerator_withoutInit_protectedSetupMethodsNotCalled() {
        DynamoDBProgrammaticKeyValue sut = spy(new DynamoDBProgrammaticKeyValue(awsCredentialsProvider,
                configuration, dynamoDBAutoscalingConfigurer));
        assertNotNull(sut);
        verify(sut, times(0)).init(any());
        verify(sut, times(0)).createAndSetDynamoDBClient();
        verify(sut, times(0)).instantiateDataPlaneOperations(any());
    }

    @TestPropertyOverride({"ndbench.config.dynamodb.autoscaling=false",
            "ndbench.config.dynamodb.endpoint=http://localhost:4567",
            "ndbench.config.dynamodb.region=us-east-1"})
    @Test
    public void testCreateDynamoDbFromDataGenerator_withInit_andNoAutoscaling_protectedSetupMethodsCalled() {
        DynamoDBProgrammaticKeyValue sut = spy(new DynamoDBProgrammaticKeyValue(awsCredentialsProvider,
                configuration, dynamoDBAutoscalingConfigurer));
        assertNotNull(sut);
        assertNotNull(sut);
        sut.init(dataGenerator);
        verify(sut, times(1)).createAndSetDynamoDBClient();
        verify(sut, times(1)).instantiateDataPlaneOperations(any());
        verify(dynamoDBAutoscalingConfigurer, times(0)).setupAutoscaling(any(), any(), any(), any(), any(), any());
    }

    @TestPropertyOverride({"ndbench.config.dynamodb.endpoint=http://localhost:4567",
            "ndbench.config.dynamodb.region=us-east-1"})
    @Test
    public void testCreateDynamoDbFromDataGenerator_withInit_andAutoscaling_protectedSetupMethodsCalled() {
        DynamoDBProgrammaticKeyValue sut = spy(new DynamoDBProgrammaticKeyValue(awsCredentialsProvider,
                configuration, dynamoDBAutoscalingConfigurer));
        assertNotNull(sut);
        sut.init(dataGenerator);
        verify(dynamoDBAutoscalingConfigurer, times(1)).setupAutoscaling(any(), any(), any(), any(), any(), any());
    }
}
