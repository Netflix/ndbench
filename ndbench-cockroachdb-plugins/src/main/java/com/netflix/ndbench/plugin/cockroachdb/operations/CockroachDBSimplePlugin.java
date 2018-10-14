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
@NdBenchClientPlugin("CockroachDBSimplePlugin")
public class CockroachDBSimplePlugin extends CockroachDBPluginBase
{
    private static String readQuery = "SELECT key, column1, value FROM %s where key = ";
    private static String writeQuery = "UPSERT INTO %s (key, column1, value) VALUES ";

    @Inject
    public CockroachDBSimplePlugin(CockroachDBConfiguration cockroachDBConfiguration) {
        super(cockroachDBConfiguration);
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        ResultSet rs = connection.createStatement().executeQuery(readQuery + "'" + key + "'");
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
        connection
        .createStatement()
        .execute(writeQuery + "('" + key + "', 1, '" + dataGenerator.getRandomValue() + "')");
        return ResultOK;
    }

    public void createTables() throws Exception
    {
        connection
        .createStatement()
        .execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (key STRING PRIMARY KEY, column1 INT, value STRING)", config.getDBName(), config.getTableName()));
    }

    public void prepareStatements()
    {
        readQuery = String.format(readQuery, config.getTableName());
        writeQuery = String.format(writeQuery, config.getTableName());
    }
}
