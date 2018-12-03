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
package com.netflix.ndbench.plugin.dynamodb.operations.controlplane;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.netflix.ndbench.plugin.dynamodb.operations.v1.dynamodb.controlplane.CreateDynamoDBTable;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateTableTest {
    private AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);

    @Test(expected = NullPointerException.class)
    public void constructor_whenDynamoDbIsNull_throwsNullPointerException() {
        new CreateDynamoDBTable(null, "asdf", "asdf", 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenTableNameIsNull_throwsIllegalArgumentException() {
        new CreateDynamoDBTable(dynamoDB, null, "asdf", 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenTableNameIsEmpty_throwsIllegalArgumentException() {
        new CreateDynamoDBTable(dynamoDB, "", "asdf", 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenPartitionKeyNameIsNull_throwsIllegalArgumentException() {
        new CreateDynamoDBTable(dynamoDB, "asdf", null, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenPartitionKeyNameIsEmpty_throwsIllegalArgumentException() {
        new CreateDynamoDBTable(dynamoDB, "asdf", "", 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenRcuLessThanOne_throwsIllegalArgumentException() {
        new CreateDynamoDBTable(dynamoDB, "asdf", "asdf", 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenWcuLessThanOne_throwsIllegalArgumentException() {
        new CreateDynamoDBTable(dynamoDB, "asdf", "asdf", 1, 0);
    }

    @Test
    public void get_whenTableAlreadyExists_doesNotCreateTable() {
        CreateDynamoDBTable createDynamoDBTable = new CreateDynamoDBTable(dynamoDB, "asdf", "asdf", 1, 1);
        //setup
        when(dynamoDB.describeTable("asdf")).thenReturn(new DescribeTableResult().withTable(new TableDescription()));

        //test
        createDynamoDBTable.get();

        //verify
        verify(dynamoDB).describeTable("asdf");
        verify(dynamoDB, Mockito.never()).createTable(any(CreateTableRequest.class));
    }

    @Test
    public void get_whenTableDoesNotExist_createsTable() {
        CreateDynamoDBTable createDynamoDBTable = new CreateDynamoDBTable(dynamoDB, "asdf", "asdf", 1, 1);
        //setup
        when(dynamoDB.describeTable("asdf")).thenThrow(new ResourceNotFoundException("asdf"));
        when(dynamoDB.createTable(any(CreateTableRequest.class)))
                .thenReturn(new CreateTableResult().withTableDescription(new TableDescription()));
        when(dynamoDB.describeTable(new DescribeTableRequest().withTableName("asdf")))
                .thenReturn(new DescribeTableResult().withTable(new TableDescription().withTableStatus(TableStatus.ACTIVE)));

        //test
        createDynamoDBTable.get();

        //verify
        verify(dynamoDB).describeTable("asdf");
        verify(dynamoDB).createTable(any(CreateTableRequest.class));
    }
}
