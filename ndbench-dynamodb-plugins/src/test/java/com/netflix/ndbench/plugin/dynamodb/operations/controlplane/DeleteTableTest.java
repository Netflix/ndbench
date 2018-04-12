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

import com.netflix.ndbench.plugin.dynamodb.operations.dynamodb.controlplane.DeleteDynamoDBTable;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import static org.mockito.Mockito.*;

public class DeleteTableTest {
    private DynamoDBClient dynamoDB = mock(DynamoDBClient.class);
    @Test
    public void delete_whenTableAlreadyExists_deletesTable() {
        DeleteDynamoDBTable deleteDynamoDBTable = new DeleteDynamoDBTable(dynamoDB, "asdf", "asdf");
        //setup
        when(dynamoDB.deleteTable(DeleteTableRequest.builder().tableName("asdf").build())).thenReturn(DeleteTableResponse.builder().build());
        when(dynamoDB.describeTable(DescribeTableRequest.builder().tableName("asdf").build())).thenThrow(ResourceNotFoundException.builder().build());

        //test
        deleteDynamoDBTable.delete();

        //verify
        verify(dynamoDB).deleteTable(DeleteTableRequest.builder().tableName("asdf").build());
    }

    @Test
    public void delete_whenTableDoesNotExist_doesNothing() {
        DeleteDynamoDBTable deleteDynamoDBTable = new DeleteDynamoDBTable(dynamoDB, "asdf", "asdf");
        //setup
        when(dynamoDB.deleteTable(DeleteTableRequest.builder().tableName("asdf").build())).thenThrow(ResourceNotFoundException.builder().build());

        //test
        deleteDynamoDBTable.delete();

        //verify
        verify(dynamoDB).deleteTable(DeleteTableRequest.builder().tableName("asdf").build());
    }
}
