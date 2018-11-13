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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
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
        ResultSet rs = connection.createStatement().executeQuery(readFromMainQuery + "'" + key + "'");
        int rsSize = 0;
        while (rs.next())
        {
            rsSize++;
        }

        if (rsSize == 0)
        {
            return CacheMiss;
        }

        if (rsSize > 1)
        {
            throw new Exception("Expecting only 1 row with a given key: " + key);
        }

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

        connection.setAutoCommit(false);
        Savepoint sp = connection.setSavepoint("cockroach_restart");

        try
        {
            Statement statement = connection.createStatement();

            statement.execute(writeToMainQuery + "('" + key + "', " + StringUtils.join(childKeys, ',') + ")");

            for (int i = 0; i < config.getColsPerRow(); i++)
            {
                connection.createStatement()
                          .execute(String.format(writeToChildQuery, i) + "(" + childKeys[i] + ", 1, '" + dataGenerator.getRandomValue() + "')");
            }

            connection.releaseSavepoint(sp);
        }
        catch (SQLException ex)
        {
            if(ex.getSQLState().equals("40001")) {
                connection.rollback(sp);
                connection.commit();
                connection.setAutoCommit(true);
                //optionally, we can retry
                return ResultFailed;
            }
        }

        connection.commit();
        connection.setAutoCommit(true);

        return ResultOK;
    }

    public void createTables() throws Exception
    {
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
    }

    public void prepareStatements()
    {
        String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i).collect(Collectors.joining(", "));
        readFromMainQuery = String.format(readFromMainQuery, columns, config.getTableName());
        writeToMainQuery = String.format(writeToMainQuery, config.getTableName(), columns);
    }
}
