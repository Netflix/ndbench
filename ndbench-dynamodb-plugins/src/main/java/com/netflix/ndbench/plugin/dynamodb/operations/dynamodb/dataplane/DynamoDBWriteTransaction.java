/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.dataplane;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.netflix.ndbench.api.plugin.DataGenerator;

/**
 * Performs writes on main table and child tables as part of a single transaction
 *
 * @author Sumanth Pasupuleti
 */
public class DynamoDBWriteTransaction extends AbstractDynamoDBDataPlaneOperation
        implements CapacityConsumingFunction<PutItemResult, String, String> {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBWriteTransaction.class);
    private static final String ResultOK = "Ok";
    private static final String ResultFailed = "Failed";
    private String childTableNamePrefix;
    private int mainTableColsPerRow;
    public DynamoDBWriteTransaction(DataGenerator dataGenerator, AmazonDynamoDB dynamoDB, String tableName,
                                    String partitionKeyName, String childTableNamePrefix, int mainTableColsPerRow,
                                    ReturnConsumedCapacity returnConsumedCapacity) {
        super(dynamoDB, tableName, partitionKeyName, dataGenerator, returnConsumedCapacity);
        this.childTableNamePrefix = childTableNamePrefix;
        this.mainTableColsPerRow = mainTableColsPerRow;
    }

    @Override
    public String apply(String key) {
        Collection<TransactWriteItem> writes = new ArrayList<>();

        HashMap<String, AttributeValue> mainTableItem = new HashMap<>();
        mainTableItem.put(partitionKeyName, new AttributeValue(key));

        for (int i = 0; i < mainTableColsPerRow; i++)
        {
            String value = this.dataGenerator.getRandomValue();

            // main table entry
            mainTableItem.put(childTableNamePrefix + i, new AttributeValue(value));

            // child table entries
            HashMap<String, AttributeValue> childTableItem = new HashMap<>();
            childTableItem.put(partitionKeyName, new AttributeValue(value));
            Put childTableEntry = new Put()
                                  .withTableName(childTableNamePrefix + i)
                                  .withItem(childTableItem)
                                  .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD);
            writes.add(new TransactWriteItem().withPut(childTableEntry));
        }

        Put mainTableEntry = new Put()
                             .withTableName(tableName)
                             .withItem(mainTableItem)
                             .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD);

        writes.add(new TransactWriteItem().withPut(mainTableEntry));
        TransactWriteItemsRequest placeWriteTransaction = new TransactWriteItemsRequest()
                                                          .withTransactItems(writes)
                                                          .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        try
        {
            dynamoDB.transactWriteItems(placeWriteTransaction);
            return ResultOK;
        }
        catch (ResourceNotFoundException rnf)
        {
            logger.error("One of the table involved in the transaction is not found" + rnf.getMessage());
        }
        catch (InternalServerErrorException ise)
        {
            logger.error("Internal Server Error" + ise.getMessage());
        }
        catch (TransactionCanceledException tce)
        {
            logger.warn("Transaction Canceled " + tce.getMessage());
        }
        return ResultFailed;
    }

    @Override
    public PutItemResult measureConsumedCapacity(PutItemResult result) {
        ConsumedCapacity consumedCapacity = result.getConsumedCapacity();
        if (consumedCapacity != null && consumedCapacity.getCapacityUnits() != null) {
            consumed.addAndGet(result.getConsumedCapacity().getCapacityUnits());
        }
        return result;
    }
}
