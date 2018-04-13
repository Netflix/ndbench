/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vchella
 */
public abstract class CJavaDriverBasePlugin implements NdBenchClient {

    private static final Logger logger = LoggerFactory.getLogger(CJavaDriverBasePlugin.class);

    protected static final String ResultOK = "Ok";
    protected static final String ResutlFailed = "Failed";
    protected static final String CacheMiss = null;

    protected DataGenerator dataGenerator;
    protected PropertyFactory propertyFactory;

    // settings
    protected static String ClusterName, KeyspaceName, TableName, ClusterContactPoint;
    int connections, port;

    protected ConsistencyLevel WriteConsistencyLevel=ConsistencyLevel.LOCAL_ONE, ReadConsistencyLevel=ConsistencyLevel.LOCAL_ONE;


    protected final CassJavaDriverManager cassJavaDriverManager;
    protected Cluster cluster;
    protected Session session;
    protected PreparedStatement readPstmt;
    protected PreparedStatement writePstmt;
    Long MaxColCount;

    @Inject
    public CJavaDriverBasePlugin(PropertyFactory propertyFactory, CassJavaDriverManager javaDriverManager) {
        this.propertyFactory = propertyFactory;
        this.cassJavaDriverManager = javaDriverManager;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;

        ClusterName = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.cluster").asString("localhost").get();
        ClusterContactPoint = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.host").asString("127.0.0.1").get();
        KeyspaceName = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.keyspace").asString("dev1").get();
        TableName =propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.cfname").asString("emp").get();
        port = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE + "cass.host.port").asInteger(9042).get();
        connections = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.connections").asInteger(2).get();

        ReadConsistencyLevel = ConsistencyLevel.valueOf(propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.readConsistencyLevel").asString(ConsistencyLevel.LOCAL_ONE.toString()).get());
        WriteConsistencyLevel = ConsistencyLevel.valueOf(propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.writeConsistencyLevel").asString(ConsistencyLevel.LOCAL_ONE.toString()).get());

       MaxColCount  = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.colsPerRow").asLong(100L).get();

       preInit();
       initDriver();
       postInit();
       prepStatements(this.session);
    }


    @Override
    public void shutdown() throws Exception {
            this.cassJavaDriverManager.shutDown();
    }

    @Override
    public String getConnectionInfo() throws Exception {
        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ReadCL - %s : WriteCL - %s ", ClusterName, KeyspaceName, TableName
                ,ReadConsistencyLevel,WriteConsistencyLevel);
    }

    @Override
    public String runWorkFlow() throws Exception {
        return null;
    }

    private void initDriver() throws Exception {

        logger.info("Cassandra  Cluster: " + ClusterName);

        this.cluster = cassJavaDriverManager.registerCluster(ClusterName,ClusterContactPoint,connections,port);
        this.session = cassJavaDriverManager.getSession(cluster);

        upsertKeyspace(this.session);
        upsertCF(this.session);
    }

   abstract void prepStatements(Session session);
   abstract void upsertKeyspace(Session session);
   abstract void upsertCF(Session session);
   abstract void preInit();
   abstract void postInit();

   protected void upsertGenereicKeyspace()
   {
       session.execute("CREATE KEYSPACE IF NOT EXISTS " +KeyspaceName+" WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};");
       session.execute("Use " + KeyspaceName);
   }
}
