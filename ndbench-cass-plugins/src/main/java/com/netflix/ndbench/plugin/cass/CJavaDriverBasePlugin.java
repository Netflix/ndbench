/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.configs.CassandraConfigurationBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author vchella
 * @author Alexander Patrikalakis
 */
public abstract class CJavaDriverBasePlugin<C extends CassandraConfigurationBase> implements NdBenchClient {

    private static final Logger logger = LoggerFactory.getLogger(CJavaDriverBasePlugin.class);

    protected static final String ResultOK = "Ok";
    protected static final String ResutlFailed = "Failed";
    protected static final String CacheMiss = null;

    protected final CassJavaDriverManager cassJavaDriverManager;
    protected final C config;

    // settings
    protected volatile DataGenerator dataGenerator;
    protected volatile String ClusterName;
    protected volatile String KeyspaceName;
    protected volatile String TableName;
    protected volatile String ClusterContactPoint;
    protected volatile String username;
    protected volatile String password;
    protected volatile int connections;
    protected volatile int port;
    protected volatile ConsistencyLevel WriteConsistencyLevel = ConsistencyLevel.LOCAL_ONE;
    protected volatile ConsistencyLevel ReadConsistencyLevel = ConsistencyLevel.LOCAL_ONE;
    protected volatile Long MaxColCount;

    protected volatile Cluster cluster;
    protected volatile Session session;
    protected volatile PreparedStatement readPstmt;
    protected volatile PreparedStatement writePstmt;


    /**
     * Creates an instance of the abstract CJavaDriverBasePlugin class. Subclasses calling this method should use
     * Guice injection to provide the arguments.
     * @param javaDriverManager
     * @param config
     */
    protected CJavaDriverBasePlugin(CassJavaDriverManager javaDriverManager, C config) {
        this.cassJavaDriverManager = javaDriverManager;
        this.config = config;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        this.ClusterName = config.getCluster();
        this.ClusterContactPoint = config.getHost();
        this.port = config.getHostPort();
        this.KeyspaceName = config.getKeyspace();
        this.connections = config.getConnections();
        this.username = config.getUsername();
        this.password = config.getPassword();

        // we do not set ReadConsistencyLevel and WriteConsistencyLevel and MaxColCount here because the
        // enum classes corresponding to the consistency levels differ among the concrete subclasses and because
        // the configs for the concrete subclasses indicate a different default value for MaxColCount.

        preInit();
        initDriver();
        postInit();
        prepStatements(this.session);
    }


    @Override
    public void shutdown() {
        this.cassJavaDriverManager.shutDown();
    }

    @Override
    public String getConnectionInfo() {
        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ReadCL - %s : WriteCL - %s ",
                ClusterName, KeyspaceName, TableName, ReadConsistencyLevel, WriteConsistencyLevel);
    }

    @Override
    public String runWorkFlow() {
        return null;
    }

    private void initDriver() {
        logger.info("Cassandra  Cluster: " + ClusterName);

        this.cluster = cassJavaDriverManager.registerCluster(ClusterName, ClusterContactPoint, connections, port,
                username, password);
        this.session = cassJavaDriverManager.getSession(cluster);

        upsertKeyspace(this.session);
        upsertCF(this.session);
    }

    abstract void prepStatements(Session session);
    abstract void upsertKeyspace(Session session);
    abstract void upsertCF(Session session);
    void preInit() {

    }
    void postInit() {

    }

    /**
     * Perform a bulk read operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> readBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    /**
     * Perform a bulk write operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> writeBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    protected void upsertGenereicKeyspace() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " +KeyspaceName+" WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};");
        session.execute("Use " + KeyspaceName);
    }
}
