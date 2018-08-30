/*
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.ndbench.plugin.cass;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.configs.CassandraAstynaxConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("CassAstyanaxPlugin")
public class CassAstyanaxPlugin implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(CassAstyanaxPlugin.class);
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    @Inject
    private CassandraAstynaxConfiguration config;

    private volatile DataGenerator dataGenerator;
    private volatile String ClusterName;
    private volatile String ClusterContactPoint;
    private volatile String KeyspaceName;
    private volatile String ColumnFamilyName;
    private volatile ConsistencyLevel WriteConsistencyLevel;
    private volatile ConsistencyLevel ReadConsistencyLevel;
    private volatile long MaxColCount;

    private volatile ColumnFamily<String, Integer> CF;
    private volatile AstyanaxContext<Keyspace> context;
    private volatile Keyspace keyspace;

    /**
     * Initialize the client
     */
    @Override
    public void init(DataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;

        ClusterName = config.getCluster();
        logger.info("Cassandra  Cluster: " + ClusterName);
        ClusterContactPoint = config.getHost();
        KeyspaceName = config.getKeyspace();

        //the defaults for these configuration options differ between cassandra implementations
        ColumnFamilyName = config.getCfname();
        ReadConsistencyLevel  = ConsistencyLevel.valueOf(config.getReadConsistencyLevel());
        WriteConsistencyLevel = ConsistencyLevel.valueOf(config.getReadConsistencyLevel());
        MaxColCount = config.getColsPerRow();

        //ColumnFamily Definition
        CF = new ColumnFamily<>(ColumnFamilyName, StringSerializer.get(), IntegerSerializer.get(), StringSerializer.get());

        context = new AstyanaxContext.Builder()
            .forCluster(ClusterName)
            .forKeyspace(KeyspaceName)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                    .setPort(config.getHostPort())
                    .setMaxConnsPerHost(1)
                    .setSeeds(ClusterContactPoint)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace = context.getClient();

        logger.info("Initialized CassAstyanaxPlugin");
    }

    /**
     * Perform a single read operation
     *
     * @param key
     * @return
     * @throws  Exception This could throw exceptions when there are exceptions in read path
     */
    @Override
    public String readSingle(String key) throws Exception {

        ColumnList<Integer> result = keyspace.prepareQuery(this.CF)
                .setConsistencyLevel(this.ReadConsistencyLevel)
                .getRow(key)
                .execute().getResult();

        if (!result.isEmpty()) {
            if (result.size() < (this.MaxColCount)) {
                throw new Exception("Num Cols returned not ok " + result.size());
            }
        } else {
            return CacheMiss;
        }

        return ResultOK;
    }

    /**
     * Perform a single write operation
     *
     * @param key
     * @return
     * @throws Exception could throw exceptions when there are exceptions in write path
     */
    @Override
    public String writeSingle(String key) throws Exception {
        MutationBatch m = keyspace.prepareMutationBatch().withConsistencyLevel(this.WriteConsistencyLevel);

        ColumnListMutation<Integer> colsMutation = m.withRow(this.CF, key);

        for (int i = 0; i < this.MaxColCount; i++) {
            colsMutation.putColumn(i, dataGenerator.getRandomValue());
        }

        m.execute();

        return ResultOK;
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        logger.info("Shutting down CassAstyanaxPlugin");
        context.shutdown();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ReadCL - %s : WriteCL - %s ", ClusterName, KeyspaceName, ColumnFamilyName, ReadConsistencyLevel, WriteConsistencyLevel);
    }

    /**
     * Run workflow for functional testing
     *
     * @throws Exception
     */
    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

}
