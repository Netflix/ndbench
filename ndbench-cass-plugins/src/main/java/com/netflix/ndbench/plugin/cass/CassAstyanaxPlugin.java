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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.model.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.util.CheckSumUtil;
import com.netflix.ndbench.plugin.cass.astyanax.CassA6XManager;
import com.netflix.ndbench.plugin.configs.CassandraAstynaxConfiguration;

/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("CassAstyanaxPlugin")
public class CassAstyanaxPlugin implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(CassAstyanaxPlugin.class);
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    private final IConfiguration coreConfig;
    private final CassandraAstynaxConfiguration config;
    private final CassA6XManager cassA6XManager;

    private volatile DataGenerator dataGenerator;

    private volatile ColumnFamily<String, Integer> CF;
    private volatile Keyspace keyspace;
    private volatile Cluster cluster;

    @Inject
    public CassAstyanaxPlugin(CassA6XManager cassA6XManager, IConfiguration coreConfig, CassandraAstynaxConfiguration config) {
        this.cassA6XManager = cassA6XManager;
        this.coreConfig = coreConfig;
        this.config = config;
    }

    /**
     * Initialize the client
     */
    @Override
    public void init(DataGenerator dataGenerator) throws Exception
    {
        try
        {
            this.dataGenerator = dataGenerator;
            this.cluster = cassA6XManager.registerCluster(config.getCluster(), config.getHost(), config.getHostPort());
            this.keyspace = cassA6XManager.registerKeyspace(config.getCluster(), config.getKeyspace(), config.getHost(), config.getHostPort());

            AstyanaxConfigurationImpl aci = (AstyanaxConfigurationImpl) this.keyspace.getConfig();
            aci.setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(config.getReadConsistencyLevel()))
               .setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(config.getWriteConsistencyLevel()));

            CF = new ColumnFamily<>(config.getCfname(), StringSerializer.get(), IntegerSerializer.get(), StringSerializer.get());

            if (config.getCreateSchema())
            {
                logger.info("Trying to upsert schema");
                preInit();
            }
        }
        catch (ConnectionException e)
        {
            logger.error("Failed to initialize Astyanax driver");
            throw e;
        }

        logger.info("Registered keyspace : " + this.keyspace.getKeyspaceName());
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
                .setConsistencyLevel(ConsistencyLevel.valueOf(config.getReadConsistencyLevel()))
                .getRow(key)
                .execute().getResult();

        if (!result.isEmpty()) {
            if (result.size() < (config.getColsPerRow())) {
                throw new Exception("Num Cols returned not ok " + result.size());
            }

            if (coreConfig.isValidateChecksum())
            {
                for (int i = 0; i < result.size(); i++)
                {
                    Column<Integer> column = result.getColumnByIndex(i);

                    // validate column name
                    if (column.getName() != i)
                    {
                        throw new Exception(String.format("Column name %d does not match with the expected column name %d", column.getName(), i));
                    }

                    // validate column value checksum
                    String value = column.getStringValue();
                    if (!CheckSumUtil.isChecksumValid(value))
                    {
                        throw new Exception(String.format("Value %s is corrupt. Key %s.", value, key));
                    }
                }
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
        MutationBatch m = keyspace.prepareMutationBatch().withConsistencyLevel(ConsistencyLevel.valueOf(config.getWriteConsistencyLevel()));

        ColumnListMutation<Integer> colsMutation = m.withRow(this.CF, key);

        for (int i = 0; i < config.getColsPerRow(); i++) {
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
        cassA6XManager.shutDown();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ColsPerRow - %s : ReadCL - %s : WriteCL - %s ", config.getCluster(), config.getKeyspace(), config.getCfname()
        , config.getColsPerRow(), config.getReadConsistencyLevel(), config.getWriteConsistencyLevel());

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

    protected void upsertKeyspace(String keyspaceName) throws Exception {
        boolean keyspaceExist = false, cfExist = false;
        for (KeyspaceDefinition ks : cluster.describeKeyspaces()) {
            if (ks.getName().equalsIgnoreCase(keyspaceName)) {
                keyspaceExist = true;
                logger.info("Keyspace ->  Name : " + keyspaceName + " already exists.");
                break;
            }
        }
        if (!keyspaceExist) {
            KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();
            ksDef.setName(keyspaceName);
            ksDef.setStrategyClass("SimpleStrategy");
            Map<String, String> options = new HashMap<String, String>();
            options.put("replication_factor", "1");
            ksDef.setStrategyOptions(options);
            cluster.addKeyspace(ksDef);

            logger.info("Created Keyspace ->  Name : " + keyspaceName);

        }
    }

    protected void upsertColumnFamily(String keyspaceName, String cfName) throws ConnectionException {
        if (cluster.describeKeyspace(keyspaceName).getColumnFamily(cfName) == null) {
            ColumnFamilyDefinition cfDef = cluster.makeColumnFamilyDefinition();

            cfDef.setComment("CF Created from NdBench")
                 .setKeyspace(keyspaceName)
                 .setName(cfName)
                 .setComparatorType("Int32Type")
                 .setKeyValidationClass("UTF8Type")
                 .setDefaultValidationClass("UTF8Type")
                 .setLocalReadRepairChance(Double.parseDouble("0"))
                 .setReadRepairChance(Double.parseDouble("0"))
                 .setCompactionStrategy("SizeTieredCompactionStrategy")
                 .setFieldValue("MEMTABLE_FLUSH_PERIOD_IN_MS", 60000)
                 .setFieldValue("INDEX_INTERVAL", 256)
                 .setFieldValue("SPECULATIVE_RETRY", "NONE")
                 .setCompressionOptions(ImmutableMap.<String, String>builder().put("sstable_compression", "").build());

            cluster.addColumnFamily(cfDef);
            logger.info("Created ColumnFamily ->  Name : " + cfName + " Definition: " + cfDef.toString());
        } else {
            logger.info("ColumnFamily ->  Name : " + cfName + " already exists.");
        }
    }


    protected void preInit() throws Exception
    {
        try {
            upsertKeyspace(config.getKeyspace());
            upsertColumnFamily(config.getKeyspace(), config.getCfname());
        } catch (Exception e) {
            logger.error("Failed to upsert keyspace/ CF.", e);
        }
    }
}
