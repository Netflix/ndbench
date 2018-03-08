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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.dynamodb.configs.DynamoDBConfigs;

/**
 * This NDBench plugin provides a single key value for AWS DynamoDB.
 * 
 * @author ipapapa
 */
@Singleton
@NdBenchClientPlugin("DynamoDBKeyValueWithTableCreation")
public class DynamoDBKeyValue implements NdBenchClient {
    private final Logger logger = LoggerFactory.getLogger(DynamoDBKeyValue.class);
    private static AmazonDynamoDB client;
    private static AWSCredentialsProvider awsCredentialsProvider;
    private static Table table;

    private DynamoDBConfigs config;
    private DataGenerator dataGenerator;

    /**
     * Credentials will be loaded based on the environment. In AWS, the credentials
     * are based on the instance. In a local deployment they will have to provided.
     */
    @Inject
    public DynamoDBKeyValue(AWSCredentialsProvider credential, DynamoDBConfigs config) {
	this.config = config;
	// if (System.getenv(NdBenchConstants.DISCOVERY_ENV).equals("AWS")) {
	awsCredentialsProvider = credential;
	/**
	 * } else { awsCredentialsProvider = new ProfileCredentialsProvider(); try {
	 * awsCredentialsProvider.getCredentials(); } catch (Exception e) { throw new
	 * AmazonClientException("Cannot load the credentials from the credential
	 * profiles file. " + "Please make sure that your credentials file is at the
	 * correct " + "location (/home/<username>/.aws/credentials), and is in valid
	 * format.", e); } }
	 **/
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;

	logger.info("Initing DynamoDB plugin");
	client = AmazonDynamoDBClientBuilder.standard().withCredentials(awsCredentialsProvider).build();

	if (this.config.programTables()) {
	    logger.info("Creating table programmatically");
	    initializeTable();
	}

	DescribeTableRequest describeTableRequest = new DescribeTableRequest()
		.withTableName(this.config.getTableName());
	TableDescription tableDescription = client.describeTable(describeTableRequest).getTable();
	logger.info("Table Description: " + tableDescription);

	DynamoDB dynamoDB = new DynamoDB(client);
	table = dynamoDB.getTable(this.config.getTableName());
	
	logger.info("DynamoDB Plugin initialized");
    }

    /**
     * 
     * @param key
     * @return the item
     * @throws Exception
     */
    @Override
    public String readSingle(String key) throws Exception {
	Item item = null;
	try {
	    GetItemSpec spec = new GetItemSpec().withPrimaryKey("Id", key).withConsistentRead(config.consistentRead());
	    item = table.getItem(spec);
	    if (item == null) {
		return null;
	    }
	} catch (AmazonServiceException ase) {
	    amazonServiceException(ase);
	} catch (AmazonClientException ace) {
	    amazonClientException(ace);
	}
	return item.toString();
    }

    /**
     * 
     * @param key
     * @return A string representation of the output of a PutItemOutcome operation.
     * @throws Exception
     */
    @Override
    public String writeSingle(String key) throws Exception {
	PutItemOutcome outcome = null;
	try {
	    Item item = new Item()
		    .withPrimaryKey("id", key)
		    .withString("value", this.dataGenerator.getRandomValue());
	    // Write the item to the table
	    outcome = table.putItem(item);
	    if (outcome == null) {
		return null;
	    }

	} catch (AmazonServiceException ase) {
	    amazonServiceException(ase);
	} catch (AmazonClientException ace) {
	    amazonClientException(ace);
	}
	return outcome.toString();
    }

    @Override
    public List<String> readBulk(List<String> keys) throws Exception {
	return null;
    }

    @Override
    public List<String> writeBulk(List<String> keys) throws Exception {
	return null;
    }

    @Override
    public void shutdown() throws Exception {
	if(this.config.programTables()) {
	    deleteTable();
	}
	client.shutdown();
	logger.info("DynamoDB shutdown");
    }

    /*
     * Not needed for this plugin
     * 
     * @see com.netflix.ndbench.api.plugin.NdBenchClient#getConnectionInfo()
     */
    @Override
    public String getConnectionInfo() throws Exception {
	return null;
    }

    @Override
    public String runWorkFlow() throws Exception {
	return null;
    }

    private void amazonServiceException(AmazonServiceException ase) {

	logger.error("Caught an AmazonServiceException, which means your request made it "
		+ "to AWS, but was rejected with an error response for some reason.");
	logger.error("Error Message:    " + ase.getMessage());
	logger.error("HTTP Status Code: " + ase.getStatusCode());
	logger.error("AWS Error Code:   " + ase.getErrorCode());
	logger.error("Error Type:       " + ase.getErrorType());
	logger.error("Request ID:       " + ase.getRequestId());
    }

    private void amazonClientException(AmazonClientException ace) {
	logger.error("Caught an AmazonClientException, which means the client encountered "
		+ "a serious internal problem while trying to communicate with AWS, "
		+ "such as not being able to access the network.");
	logger.error("Error Message: " + ace.getMessage());
    }
    
    private void initializeTable() {
	/*
	 * Create a table with a primary hash key named 'name', which holds a string.
	 * Several properties such as provisioned throughput and atribute names are
	 * defined in the configuration interface.
	 */

	logger.debug("Creating table if it does not exist yet");

	Long readCapacityUnits = Long.parseLong(this.config.getReadCapacityUnits());
	Long writeCapacityUnits = Long.parseLong(this.config.getWriteCapacityUnits());

	// key schema
	ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
	keySchema.add(new KeySchemaElement().withAttributeName(config.getAttributeName()).withKeyType(KeyType.HASH));

	// Attribute definitions
	ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
	attributeDefinitions.add(new AttributeDefinition().withAttributeName(config.getAttributeName())
		.withAttributeType(ScalarAttributeType.S));
	/*
	 * constructing the table request: Schema + Attributed definitions + Provisioned
	 * throughput
	 */
	CreateTableRequest request = new CreateTableRequest().withTableName(this.config.getTableName())
		.withKeySchema(keySchema).withAttributeDefinitions(attributeDefinitions)
		.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits)
			.withWriteCapacityUnits(writeCapacityUnits));

	logger.info("Creating Table: " + this.config.getTableName());

	// Creating table
	if (TableUtils.createTableIfNotExists(client, request))
	    logger.info("Table already exists.  No problem!");
	
	// Waiting util the table is ready
	try {
	    logger.debug("Waiting until the table is in ACTIVE state");
	    TableUtils.waitUntilActive(client, this.config.getTableName());
	} catch (AmazonClientException e) {
	    logger.error("Table didn't become active: " + e);
	} catch (InterruptedException e) {
	    logger.error("Table interrupted exception: " + e);
	}	
    }
    
    private void deleteTable() {
	try {
	    logger.info("Issuing DeleteTable request for " + config.getTableName());
	    table.delete();

	    logger.info("Waiting for " + config.getTableName() + " to be deleted...this may take a while...");

	    table.waitForDelete();
	} catch (Exception e) {
	    logger.error("DeleteTable request failed for " + config.getTableName());
	    logger.error(e.getMessage());
	}
	table.delete(); // cleanup
    }
}
