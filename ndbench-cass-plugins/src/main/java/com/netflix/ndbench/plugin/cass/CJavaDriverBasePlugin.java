/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.plugin.configs.CassandraConfigurationBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    protected final IConfiguration coreConfig;

    // settings
    protected volatile DataGenerator dataGenerator;
    protected volatile String clusterName;
    protected volatile String keyspaceName;
    protected volatile String tableName;
    protected volatile String clusterContactPoint;
    protected volatile String username;
    protected volatile String password;
    protected volatile int connections;
    protected volatile int port;
    protected volatile Cluster cluster;
    protected volatile Session session;
    protected volatile PreparedStatement readPstmt;
    protected volatile PreparedStatement writePstmt;
    protected volatile String truststorePath;
    protected volatile String truststorePass;

    /**
     * Creates an instance of the abstract CJavaDriverBasePlugin class. Subclasses calling this method should use
     * Guice injection to provide the arguments.
     * @param javaDriverManager
     * @param config
     */
    protected CJavaDriverBasePlugin(CassJavaDriverManager javaDriverManager, IConfiguration coreConfig, C config) {
        this.cassJavaDriverManager = javaDriverManager;
        this.coreConfig = coreConfig;
        this.config = config;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        this.clusterName = config.getCluster();
        this.clusterContactPoint = config.getHost();
        this.port = config.getHostPort();
        this.keyspaceName = config.getKeyspace();
        this.tableName = config.getCfname();
        this.connections = config.getConnections();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.truststorePath = config.getTruststorePath();
        this.truststorePass = config.getTruststorePass();

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
                             clusterName, keyspaceName, tableName, config.getReadConsistencyLevel(), config.getWriteConsistencyLevel());
    }

    @Override
    public String runWorkFlow() {
        return null;
    }

    private void initDriver() {
        logger.info("Cassandra  Cluster: " + clusterName);

        this.cluster = cassJavaDriverManager.registerCluster(clusterName, clusterContactPoint, connections, port,
                                                             username, password, truststorePath, truststorePass);
        this.session = cassJavaDriverManager.getSession(cluster);
        if(config.getCreateSchema())
        {
            logger.info("Trying to upsert schema");
            upsertKeyspace(this.session);
            upsertCF(this.session);
        }
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

    protected void upsertGenereicKeyspace(Session session) {
        Set<Host> hosts = session.getCluster().getMetadata().getAllHosts();

        Set<String> dcs = hosts.stream()
                               .map(h -> "'"+h.getDatacenter()+"': '3'" )
                               .collect(Collectors.toSet());

        String rf = "{'class': 'SimpleStrategy','replication_factor': '1'}";

        if(hosts.size() > 1)
        {
            rf = String.format("{'class': 'NetworkTopologyStrategy', %s}", String.join(", ", dcs));
        }

        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s;", keyspaceName, rf));
        session.execute("Use " + keyspaceName);
    }
}
