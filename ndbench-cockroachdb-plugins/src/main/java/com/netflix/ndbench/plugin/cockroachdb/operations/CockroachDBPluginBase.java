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
package com.netflix.ndbench.plugin.cockroachdb.operations;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.cockroachdb.configs.CockroachDBConfiguration;

/**
 * @author Sumanth Pasupuleti
 */
public abstract class CockroachDBPluginBase implements NdBenchClient
{
    protected static final String ResultOK = "Ok";
    protected static final String ResultFailed = "Failed";
    protected static final String CacheMiss = null;
    private static final Logger logger = LoggerFactory.getLogger(CockroachDBPluginBase.class);

    protected Connection connection;
    protected DataGenerator dataGenerator;

    protected final CockroachDBConfiguration config;

    protected CockroachDBPluginBase(CockroachDBConfiguration cockroachDBConfiguration) {
        this.config = cockroachDBConfiguration;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception
    {
        this.dataGenerator = dataGenerator;
        logger.info("Initializing the  CockroachDB client");
        // Load the Postgres JDBC driver.
        Class.forName("org.postgresql.Driver");

        Properties props = new Properties();
        props.setProperty("user", config.getUser());

        try
        {
            connection = DriverManager.getConnection(String.format("jdbc:postgresql://%s:80/%s", config.getLoadBalancer(), config.getDBName()), props);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Exception during connection initialization", e);
        }

        logger.info("Connected to cockroach db, initilizing/ creating the table");
        createTables();
        logger.info("Created tables");
        prepareStatements();
    }

    /**
     * Shutdown the client
     */
    @Override
    public void shutdown() throws Exception
    {
        try
        {
            connection.close();
        }
        catch (SQLException e)
        {
            logger.error("Failed to close connection", e);
        }
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception
    {
        return String.format("Connected to database: %s using driver: %s as user :%s",
                             connection.getMetaData().getDatabaseProductName(),
                             connection.getMetaData().getDriverName(),
                             connection.getMetaData().getUserName());
    }

    @Override
    public String runWorkFlow() throws Exception
    {
        return null;
    }

    public abstract void createTables() throws Exception;

    public abstract void prepareStatements();

    /**
     * Assumes delimiter to be comma since that covers all the usecase for now.
     * Will parameterize if use cases differ on delimiter.
     * @param n
     * @return
     */
    public String getNDelimitedStrings(int n)
    {
        return IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "'" + dataGenerator.getRandomValue() + "'").collect(Collectors.joining(","));
    }
}
