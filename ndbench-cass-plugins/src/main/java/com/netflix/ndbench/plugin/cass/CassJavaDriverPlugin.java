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

import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("CassJavaDriverPlugin")
public class CassJavaDriverPlugin implements NdBenchClient{
    private static final Logger Logger = LoggerFactory.getLogger(CassJavaDriverPlugin.class);
    private final PropertyFactory propertyFactory;

    private Cluster cluster;
    private Session session;

    private DataGenerator dataGenerator;

    private String ClusterName , ClusterContactPoint , KeyspaceName , TableName ;
    private ConsistencyLevel WriteConsistencyLevel=ConsistencyLevel.LOCAL_ONE, ReadConsistencyLevel=ConsistencyLevel.LOCAL_ONE;

    private PreparedStatement readPstmt;
    private PreparedStatement writePstmt;

    private static final String ResultOK = "Ok";
    private static final String CacheMiss = null;

    @Inject
    public CassJavaDriverPlugin(PropertyFactory propertyFactory)
    {
        this.propertyFactory = propertyFactory;
    }
    /**
     * Initialize the client
     *
     * @throws Exception
     */
    @Override
    public void init(DataGenerator dataGenerator) throws Exception {

        ClusterName = propertyFactory.getProperty("ndbench.config.cass.cluster").asString("localhost").get();
        ClusterContactPoint = propertyFactory.getProperty("ndbench.config.cass.host").asString("127.0.0.1").get();
        KeyspaceName = propertyFactory.getProperty("ndbench.config.cass.keyspace").asString("dev1").get();
        TableName =propertyFactory.getProperty("ndbench.config.cass.cfname").asString("emp").get();

        Logger.info("Cassandra  Cluster: " + ClusterName);
        this.dataGenerator = dataGenerator;
        cluster = Cluster.builder()
                .withClusterName(ClusterName)
                .addContactPoint(ClusterContactPoint)
                .build();
        session = cluster.connect();

        upsertKeyspace(this.session);
        upsertCF(this.session);

        writePstmt = session.prepare("INSERT INTO "+ TableName +" (emp_uname, emp_first, emp_last, emp_dept ) VALUES (?, ?, ?, ? )");
        readPstmt = session.prepare("SELECT * From "+ TableName +" Where emp_uname = ?");

        Logger.info("Initialized CassJavaDriverPlugin");
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
        bStmt.setString("emp_uname", key);
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
        bStmt.setString("emp_uname", key);
        bStmt.setString("emp_first", this.dataGenerator.getRandomValue());
        bStmt.setString("emp_last", this.dataGenerator.getRandomValue());
        bStmt.setString("emp_dept", this.dataGenerator.getRandomValue());
        bStmt.setConsistencyLevel(this.WriteConsistencyLevel);

        session.execute(bStmt);
        return ResultOK;
    }

    /**
     * shutdown the client
     */
    @Override
    public void shutdown() throws Exception {
        Logger.info("Shutting down CassJavaDriverPlugin");
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
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KeyspaceName +" WITH replication = {'class':'SimpleStrategy','replication_factor':1};");
        session.execute("Use " + KeyspaceName);
    }
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS "+ TableName +" (emp_uname varchar primary key, emp_first varchar, emp_last varchar, emp_dept varchar);");

    }
}
