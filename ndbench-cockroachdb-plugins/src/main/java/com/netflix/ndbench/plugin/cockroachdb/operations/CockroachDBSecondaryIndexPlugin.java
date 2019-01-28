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

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.cockroachdb.configs.CockroachDBConfiguration;

/**
 * @author Sumanth Pasupuleti
 */

@Singleton
@NdBenchClientPlugin("CockroachDBSecondaryIndexPlugin")
public class CockroachDBSecondaryIndexPlugin extends CockroachDBPluginBase
{
    private static String readFromMainQuery = "SELECT key, %s FROM %s where key = ";
    private static String writeToMainQuery = "UPSERT INTO %s (key, %s) VALUES ";

    @Inject
    public CockroachDBSecondaryIndexPlugin(CockroachDBConfiguration cockroachDBConfiguration) {
        super(cockroachDBConfiguration);
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        Connection connection = null;
        try
        {
            connection = ds.getConnection();
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
        catch (Exception ex)
        {
            if (connection != null)
            {
                connection.close();
            }
            throw ex;
        }
    }

    @Override
    public String writeSingle(String key) throws Exception
    {
        Connection connection = null;
        try
        {
            String columns = getNDelimitedStrings(config.getColsPerRow());
            connection = ds.getConnection();
            connection
            .createStatement()
            .executeUpdate(writeToMainQuery + "('" + key + "', " + columns + ")");
            connection.close();
            return ResultOK;
        }
        catch (Exception ex)
        {
            if (connection != null)
            {
                connection.close();
            }
            throw ex;
        }
    }

    public void createTables() throws Exception
    {
        Connection connection = null;
        try
        {
            connection = ds.getConnection();
            String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i + " STRING").collect(Collectors.joining(", "));
            connection
            .createStatement()
            .execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (key STRING PRIMARY KEY, %s)", config.getDBName(), config.getTableName(), columns));

            // create secondary indices
            for (int i = 0; i < config.getColsPerRow(); i++)
            {
                connection
                .createStatement()
                .execute(String.format("CREATE INDEX IF NOT EXISTS %s_column%d_index on %s (column%d)", config.getTableName(), i, config.getTableName(), i));
            }

            connection.close();
        }
        catch (Exception ex)
        {
            if (connection != null)
            {
                connection.close();
            }
            throw ex;
        }
    }

    public void prepareStatements()
    {
        String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i).collect(Collectors.joining(", "));
        readFromMainQuery = String.format(readFromMainQuery, columns, config.getTableName());
        writeToMainQuery = String.format(writeToMainQuery, config.getTableName(), columns);
    }
}
