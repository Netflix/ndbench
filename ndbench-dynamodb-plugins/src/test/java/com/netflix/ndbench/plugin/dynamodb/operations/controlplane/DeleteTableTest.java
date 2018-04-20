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
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.waiters.AmazonDynamoDBWaiters;
import com.amazonaws.waiters.Waiter;
import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DeleteDynamoDBTable;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DeleteTableTest {
    private AmazonDynamoDB dynamoDB = mock(AmazonDynamoDB.class);
    private AmazonDynamoDBWaiters waiters = mock(AmazonDynamoDBWaiters.class);
    private Waiter<DescribeTableRequest> tableNotExists = mock(Waiter.class);

    @Test
    public void delete_whenTableAlreadyExists_deletesTable() {
        DeleteDynamoDBTable deleteDynamoDBTable = new DeleteDynamoDBTable(dynamoDB, "asdf", "asdf");
        //setup
        when(dynamoDB.deleteTable("asdf")).thenReturn(new DeleteTableResult());
        when(dynamoDB.waiters()).thenReturn(waiters);
        when(waiters.tableNotExists()).thenReturn(tableNotExists);
        doNothing().when(tableNotExists).run(any());
        when(dynamoDB.describeTable("asdf")).thenThrow(new ResourceNotFoundException(""));

        //test
        deleteDynamoDBTable.delete();

        //verify
        verify(dynamoDB).deleteTable("asdf");
        verify(tableNotExists).run(any());
    }

    @Test
    public void delete_whenTableNotExistsWaiterThrows_deletesTableAndThrows() {
        DeleteDynamoDBTable deleteDynamoDBTable = new DeleteDynamoDBTable(dynamoDB, "asdf", "asdf");
        //setup
        when(dynamoDB.deleteTable("asdf")).thenReturn(new DeleteTableResult());
        when(dynamoDB.waiters()).thenReturn(waiters);
        when(waiters.tableNotExists()).thenReturn(tableNotExists);
        doThrow(new IllegalArgumentException()).when(tableNotExists).run(any());
        when(dynamoDB.describeTable("asdf")).thenThrow(new ResourceNotFoundException(""));

        //test
        try {
            deleteDynamoDBTable.delete();
            fail();
        } catch(IllegalStateException e) {
            //verify exception
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
        verify(dynamoDB).deleteTable("asdf");
        verify(tableNotExists).run(any());
    }

    @Test
    public void delete_whenTableDoesNotExist_doesNothing() {
        DeleteDynamoDBTable deleteDynamoDBTable = new DeleteDynamoDBTable(dynamoDB, "asdf", "asdf");
        //setup
        when(dynamoDB.deleteTable("asdf")).thenThrow(new ResourceNotFoundException(""));

        //test
        deleteDynamoDBTable.delete();

        //verify
        verify(dynamoDB).deleteTable("asdf");
    }
}
