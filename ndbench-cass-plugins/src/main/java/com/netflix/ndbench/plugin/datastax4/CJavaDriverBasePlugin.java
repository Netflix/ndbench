package com.netflix.ndbench.plugin.datastax4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.plugin.configs.CassandraConfigurationBase;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CJavaDriverBasePlugin<Config extends CassandraConfigurationBase> implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(com.netflix.ndbench.plugin.cass.CJavaDriverBasePlugin.class);
    protected static final String ResultOK = "Ok";
    protected static final String ResutlFailed = "Failed";
    protected static final String CacheMiss = null;
    protected final CassJavaDriverManager cassJavaDriverManager;
    protected final IConfiguration ndbConfig;
    protected final Config config;

    // settings
    protected volatile DataGenerator dataGenerator;
    protected volatile String sessionName;
    protected volatile String keyspaceName;
    protected volatile String tableName;
    protected volatile String sessionContactPoint;
    protected volatile String username;
    protected volatile String password;
    protected volatile int connections;
    protected volatile int port;
    protected volatile CqlSession session;
    protected volatile PreparedStatement readPstmt;
    protected volatile PreparedStatement writePstmt;

    protected CJavaDriverBasePlugin(
            CassJavaDriverManager cassJavaDriverManager,
            IConfiguration ndbConfig,
            Config config) {
        this.cassJavaDriverManager = cassJavaDriverManager;
        this.ndbConfig = ndbConfig;
        this.config = config;
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;
        this.sessionName = config.getCluster();
        this.sessionContactPoint = config.getHost();
        this.port = config.getHostPort();
        this.keyspaceName = config.getKeyspace();
        this.tableName = config.getCfname();
        this.connections = config.getConnections();
        this.username = config.getUsername();
        this.password = config.getPassword();

        initSession();
        prepStatements(this.session);
    }

    private void initSession() {
        logger.info("Cassandra  Cluster: " + sessionName);

        this.session = cassJavaDriverManager.getSession(sessionName, sessionContactPoint, connections, port,
                username, password);

        if(config.getCreateSchema())
        {
            logger.info("Trying to upsert schema");
            upsertKeyspace(this.session);
            upsertCF(this.session);
        }
    }

    abstract void prepStatements(CqlSession session);
    abstract void upsertKeyspace(CqlSession session);
    abstract void upsertCF(CqlSession session);

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

    @Override
    public String runWorkFlow() {
        return null;
    }

    protected void upsertGenericKeyspace(CqlSession session) {
        Collection<Node> hosts = session.getMetadata().getNodes().values();
        Set<String> dcs = hosts.stream()
                .map(Node::getDatacenter).map(this::toReplication)
                .collect(Collectors.toSet());

        String rf = "{'class': 'SimpleStrategy','replication_factor': '1'}";

        if(hosts.size() > 1)
        {
            rf = String.format("{'class': 'NetworkTopologyStrategy', %s}", String.join(", ", dcs));
        }

        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s;", keyspaceName, rf));
        session.execute("Use " + keyspaceName);
    }

    private String toReplication(String dc) {
        return "'" + dc + ": '3'";
    }


}
