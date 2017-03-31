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

import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("CassAstyanaxPlugin")
public class CassAstyanaxPlugin implements NdBenchClient{
    private static final Logger Logger = LoggerFactory.getLogger(CassAstyanaxPlugin.class);

    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;

    private DataGenerator dataGenerator;

    private final String ClusterName = "Localhost", ClusterContactPoint ="127.0.0.1",
            KeyspaceName ="dev1", ColumnFamilyName ="emp_thrift";

    private final ConsistencyLevel WriteConsistencyLevel=ConsistencyLevel.CL_LOCAL_ONE,
            ReadConsistencyLevel=ConsistencyLevel.CL_LOCAL_ONE;


    private final ColumnFamily<String, Integer> CF = new ColumnFamily<String, Integer>(ColumnFamilyName, StringSerializer.get(), IntegerSerializer.get(), StringSerializer.get());


    private final String ResultOK = "Ok";
    private final String CacheMiss = null;
    private final int MaxColCount = 5;


    /**
     * Initialize the client
     *
     * @throws Exception
     */
    @Override
    public void init(DataGenerator dataGenerator, PropertyFactory propertyFactory) throws Exception {
        Logger.info("Cassandra  Cluster: " + ClusterName);
        this.dataGenerator = dataGenerator;

         context = new AstyanaxContext.Builder()
                .forCluster(ClusterName)
                .forKeyspace(KeyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds(ClusterContactPoint+":9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace = context.getClient();

        Logger.info("Initialized CassAstyanaxPlugin");
    }

    /**
     * Perform a single read operation
     *
     * @param key
     * @return
     * @throws Exception
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
     * @throws Exception
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
        Logger.info("Shutting down CassAstyanaxPlugin");
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
