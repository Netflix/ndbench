/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.ndbench.plugin.cockroachdb.operations;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.cockroachdb.configs.CockroachDBConfiguration;

/**
 * @author Sumanth Pasupuleti
 */
@Singleton
@NdBenchClientPlugin("CockroachDBTransactionPlugin")
public class CockroachDBTransactionPlugin extends CockroachDBPluginBase
{
    private static String readFromMainQuery = "SELECT key, %s FROM %s where key = ";
    private static String writeToMainQuery = "UPSERT INTO %s (key, %s) VALUES ";
    private static String writeToChildQuery = "UPSERT INTO child%d (key, column1, value) VALUES ";

    @Inject
    public CockroachDBTransactionPlugin(CockroachDBConfiguration cockroachDBConfiguration) {
        super(cockroachDBConfiguration);
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        Connection connection = ds.getConnection();

        ResultSet rs = connection.createStatement().executeQuery(readFromMainQuery + "'" + key + "'");
        int rsSize = 0;
        while (rs.next())
        {
            rsSize++;
        }

        if (rsSize == 0)
        {
            connection.close();
            return CacheMiss;
        }

        if (rsSize > 1)
        {
            connection.close();
            throw new Exception("Expecting only 1 row with a given key: " + key);
        }

        connection.close();
        return ResultOK;
    }

    @Override
    public String writeSingle(String key) throws Exception
    {
        //execute transaction
        String[] childKeys = new String[config.getColsPerRow()];
        for (int i = 0; i < config.getColsPerRow(); i++)
        {
            childKeys[i] = "'" + dataGenerator.getRandomValue() + "'";
        }

        Connection connection = ds.getConnection();

        connection.setAutoCommit(false);

        CockroachDBRetryableTransaction transaction = conn -> {
            Statement statement = connection.createStatement();

            // write to main table
            statement.addBatch(writeToMainQuery + "('" + key + "', " + StringUtils.join(childKeys, ',') + ")");

            // writes to child tables
            for (int i = 0; i < config.getColsPerRow(); i++)
            {
                statement.addBatch(String.format(writeToChildQuery, i) + "(" + childKeys[i] + ", 1, '" + dataGenerator.getRandomValue() + "')");
            }

            statement.executeBatch();
        };

        Savepoint sp = connection.setSavepoint("cockroach_restart");

        while(true) {
            boolean releaseAttempted = false;
            try {
                transaction.run(connection);
                releaseAttempted = true;
                connection.releaseSavepoint(sp);
                break;
            }
            catch(SQLException e) {
                String sqlState = e.getSQLState();

                // Check if the error code indicates a SERIALIZATION_FAILURE.
                if(sqlState.equals("40001")) {
                    // Signal the database that we will attempt a retry.
                    connection.rollback(sp);
                } else if(releaseAttempted) {
                    connection.close();
                    // ResultAmbiguous;
                    throw e;
                } else {
                    connection.close();
                    // ResultFailed;
                    throw e;
                }
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
        connection.close();

        return ResultOK;
    }

    public void createTables() throws Exception
    {
        Connection connection = ds.getConnection();

        String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i + " STRING").collect(Collectors.joining(", "));
        connection
        .createStatement()
        .execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (key STRING PRIMARY KEY, %s)", config.getDBName(), config.getTableName(), columns));

        // create child tables
        for (int i = 0; i < config.getColsPerRow(); i++)
        {
            connection
            .createStatement()
            .execute(String.format("CREATE TABLE IF NOT EXISTS %s.child%d (key STRING PRIMARY KEY, column1 INT, value STRING)", config.getDBName(), i));
        }

        connection.close();
    }

    public void prepareStatements()
    {
        String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i).collect(Collectors.joining(", "));
        readFromMainQuery = String.format(readFromMainQuery, columns, config.getTableName());
        writeToMainQuery = String.format(writeToMainQuery, config.getTableName(), columns);
    }
}
