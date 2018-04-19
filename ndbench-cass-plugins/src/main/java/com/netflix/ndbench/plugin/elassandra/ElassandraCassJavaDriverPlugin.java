/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.plugin.elassandra;

import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.configs.ElassandraConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * This is a Elassandra (http://www.elassandra.io/) plugin using the CASS api. <BR>
 * You need make sure you install Elassandra properly on top of your Cass database.
 * More details on elassandra installation here: http://doc.elassandra.io/en/latest/installation.html
 * 
 * This plugin will create the schema and will stress test Elassandra via Datastax driver.
 * 
 * @author diegopacheco
 *
 */
@Singleton
@NdBenchClientPlugin("ElassandraCassJavaDriverPlugin")
public class ElassandraCassJavaDriverPlugin implements NdBenchClient{
    private static final Logger logger = LoggerFactory.getLogger(ElassandraCassJavaDriverPlugin.class);
    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    @Inject
    private ElassandraConfiguration configs;

    //settings
    private volatile DataGenerator dataGenerator;
    private volatile String ClusterName;
    private volatile String ClusterContactPoint;
    private volatile String KeyspaceName;
    private volatile String TableName;
    private volatile ConsistencyLevel WriteConsistencyLevel;
    private volatile ConsistencyLevel ReadConsistencyLevel;

    private volatile Cluster cluster;
    private volatile Session session;
    private volatile PreparedStatement readPstmt;
    private volatile PreparedStatement writePstmt;

    /**
     * Initialize the client
     *
     * @throws Exception
     */
    @Override
    public void init(DataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;

        this.ClusterName = configs.getCluster();
        logger.info("Cassandra  Cluster: " + ClusterName);

        this.ClusterContactPoint = configs.getHost();
        this.KeyspaceName = configs.getKeyspace();
        this.TableName = configs.getCfname();
        this.WriteConsistencyLevel = ConsistencyLevel.valueOf(configs.getWriteConsistencyLevel());
        this.ReadConsistencyLevel = ConsistencyLevel.valueOf(configs.getReadConsistencyLevel());

        cluster = Cluster.builder()
                .withClusterName(ClusterName)
                .addContactPoint(ClusterContactPoint)
                .build();
        session = cluster.connect();

        upsertKeyspace(this.session);
        upsertCF(this.session);

        writePstmt = session.prepare("INSERT INTO "+ TableName +" (\"_id\", name) VALUES (?, ?)");
        readPstmt = session.prepare("SELECT * From "+ TableName +" Where \"_id\" = ?");

        logger.info("Initialized ElassandraCassJavaDriverPlugin");
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
        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("\"_id\"", key);
        bStmt.setConsistencyLevel(this.ReadConsistencyLevel);
        ResultSet rs = session.execute(bStmt);

        List<Row> result=rs.all();
        if (!result.isEmpty())
        {
            if (result.size() != 1) {
                throw new Exception("Num Cols returned not ok " + result.size());
            }
        }
        else {
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
        BoundStatement bStmt = writePstmt.bind();
        bStmt.setString("\"_id\"", key);
        bStmt.setList("name", Arrays.asList(this.dataGenerator.getRandomValue())) ;
        bStmt.setConsistencyLevel(this.WriteConsistencyLevel);

        session.execute(bStmt);
        return ResultOK;
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        logger.info("Shutting down ElassandraCassJavaDriverPlugin");
        cluster.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ReadCL - %s : WriteCL - %s ", ClusterName, KeyspaceName, TableName, ReadConsistencyLevel, WriteConsistencyLevel);
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

    void upsertKeyspace(Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KeyspaceName +" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '1'}  AND durable_writes = true;");
        //session.execute("CREATE KEYSPACE IF NOT EXISTS " + KeyspaceName +" WITH replication = {'class':'SimpleStrategy','replication_factor': 2};");
        session.execute("Use " + KeyspaceName);
    }
    
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS "+ TableName +" (\"_id\" text PRIMARY KEY, name list<text>) WITH bloom_filter_fp_chance = 0.01 " + 
                       " AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}'" +
                       " AND comment = 'Auto-created by Elassandra' " +
                       " AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'} " +
                       " AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'} " +
                       " AND dclocal_read_repair_chance = 0.1  " +
                       " AND default_time_to_live = 0 " +
                       " AND gc_grace_seconds = 864000 " +
                       " AND max_index_interval = 2048 " +
                       " AND memtable_flush_period_in_ms = 0 " +
                       " AND min_index_interval = 128 " +
                       " AND read_repair_chance = 0.0 " +
                       " AND speculative_retry = '99.0PERCENTILE'; ");
        session.execute("CREATE CUSTOM INDEX IF NOT EXISTS elastic_external_name_idx ON customer.external (name) USING 'org.elasticsearch.cassandra.index.ExtendedElasticSecondaryIndex';");
    }
}
