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
    private static String readFromMainQuery = "SELECT key, column1, column2, column3, column4 FROM %s where key = ";
    private static String writeToMainQuery = "UPSERT INTO %s (key, column1, column2, column3, column4) VALUES ";

    @Inject
    public CockroachDBSecondaryIndexPlugin(CockroachDBConfiguration cockroachDBConfiguration) {
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
        String child1Key = dataGenerator.getRandomValue();
        String child2Key = dataGenerator.getRandomValue();
        String child3Key = dataGenerator.getRandomValue();
        String child4Key = dataGenerator.getRandomValue();

        connection
        .createStatement()
        .execute(String.format(writeToMainQuery, config.getTableName()) + "('" + key + "', '" + child1Key + "', '" + child2Key + "', '" + child3Key + "', '" + child4Key + "')");
        return ResultOK;
    }

    public void createTables() throws Exception
    {
        connection
        .createStatement()
        .execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (key STRING PRIMARY KEY, column1 STRING, column2 STRING, column3 STRING, column4 STRING)", config.getDBName(), config.getTableName()));

        //create secondary indices
        connection
        .createStatement()
        .execute(String.format("CREATE INDEX IF NOT EXISTS %s_column1_index on %s (column1)", config.getTableName(), config.getTableName()));

        connection
        .createStatement()
        .execute(String.format("CREATE INDEX IF NOT EXISTS %s_column2_index on %s (column2)", config.getTableName(), config.getTableName()));

        connection
        .createStatement()
        .execute(String.format("CREATE INDEX IF NOT EXISTS %s_column3_index on %s (column3)", config.getTableName(), config.getTableName()));

        connection
        .createStatement()
        .execute(String.format("CREATE INDEX IF NOT EXISTS %s_column4_index on %s (column4)", config.getTableName(), config.getTableName()));
    }

    public void prepareStatements()
    {
        readFromMainQuery = String.format(readFromMainQuery, config.getTableName());
//     writeQuery = String.format(writeQuery, tableName.get());
    }
}
