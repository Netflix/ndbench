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

import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.CreateDynamoDBTable;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CreateTableTest {
    private DynamoDBClient dynamoDB = mock(DynamoDBClient.class);

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
        when(dynamoDB.describeTable(DescribeTableRequest.builder().tableName("asdf").build()))
                .thenReturn(DescribeTableResponse.builder().table(TableDescription.builder().tableName("asdf").build()).build());

        //test
        createDynamoDBTable.get();

        //verify
        verify(dynamoDB).describeTable(DescribeTableRequest.builder().tableName("asdf").build());
        verify(dynamoDB, Mockito.never()).createTable(any(CreateTableRequest.class));
    }

    @Test
    public void get_whenTableDoesNotExist_createsTable() {
        CreateDynamoDBTable createDynamoDBTable = new CreateDynamoDBTable(dynamoDB, "asdf", "asdf", 1, 1);
        //setup
        when(dynamoDB.createTable(any(CreateTableRequest.class)))
                .thenReturn(CreateTableResponse.builder().tableDescription(TableDescription.builder().build()).build());
        doThrow(ResourceNotFoundException.builder().build())
                .doReturn(DescribeTableResponse.builder().table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build()).build())
                .when(dynamoDB).describeTable(DescribeTableRequest.builder().tableName("asdf").build());

        //test
        createDynamoDBTable.get();

        //verify
        verify(dynamoDB, times(2)).describeTable(DescribeTableRequest.builder().tableName("asdf").build());
        verify(dynamoDB).createTable(any(CreateTableRequest.class));
    }
}
