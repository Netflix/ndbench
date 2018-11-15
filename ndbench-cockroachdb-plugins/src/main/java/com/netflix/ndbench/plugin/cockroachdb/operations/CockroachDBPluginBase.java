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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
    protected static final String ResultAmbiguous = "Failed";
    private static final Logger logger = LoggerFactory.getLogger(CockroachDBPluginBase.class);

    protected DataGenerator dataGenerator;

    protected final CockroachDBConfiguration config;

    protected static HikariDataSource ds;

    protected CockroachDBPluginBase(CockroachDBConfiguration cockroachDBConfiguration) {
        this.config = cockroachDBConfiguration;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception
    {
        this.dataGenerator = dataGenerator;
        logger.info("Initializing the  CockroachDB client");

        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", config.getLoadBalancer());
        props.setProperty("dataSource.user", config.getUser());
        props.setProperty("dataSource.password", config.getPassword());
        props.setProperty("dataSource.databaseName", config.getDBName());
        props.setProperty("dataSource.portNumber", config.getPort());
        props.setProperty("maximumPoolSize", config.getPoolSize());
        props.setProperty("leakDetectionThreshold", "2000");


        try
        {
            ds = new HikariDataSource(new HikariConfig(props));
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
    public void shutdown()
    {
        ds.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception
    {
        Connection connection = ds.getConnection();

        String info =  String.format("Connected to database: %s using driver: %s as user :%s",
                             connection.getMetaData().getDatabaseProductName(),
                             connection.getMetaData().getDriverName(),
                             connection.getMetaData().getUserName());

        connection.close();

        return info;
    }

    @Override
    public String runWorkFlow()
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
