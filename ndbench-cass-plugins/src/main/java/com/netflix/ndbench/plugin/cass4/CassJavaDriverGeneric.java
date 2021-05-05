package com.netflix.ndbench.plugin.cass4;

import static com.netflix.ndbench.core.util.NdbUtil.humanReadableByteCount;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.util.CheckSumUtil;
import com.netflix.ndbench.plugin.QueryUtil;
import com.netflix.ndbench.plugin.configs.CassandraGenericConfiguration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Singleton
@NdBenchClientPlugin("CassJavaDriverGeneric4")
public class CassJavaDriverGeneric extends CJavaDriverBasePlugin4<CassandraGenericConfiguration> {

    protected CassJavaDriverGeneric(
            CassJavaDriverManager cassJavaDriverManager, IConfiguration ndbConfig,
            CassandraGenericConfiguration cassandraGenericConfiguration) {
        super(cassJavaDriverManager, ndbConfig, cassandraGenericConfiguration);
    }

    @Override void prepStatements(CqlSession session) {
        int nCols = config.getColsPerRow();
        String values = IntStream.range(0, nCols).mapToObj(i -> "value"+i).collect(Collectors.joining(", "));
        String bindValues = IntStream.range(0, nCols).mapToObj(i -> "?").collect(Collectors.joining(", "));
        writePstmt = session.prepare(String.format(QueryUtil.INSERT_QUERY, keyspaceName, tableName, values, bindValues));
        readPstmt = session.prepare(String.format(QueryUtil.READ_QUERY, keyspaceName, tableName));
    }

    @Override void upsertKeyspace(CqlSession session) {
        upsertGenericKeyspace(session);
    }

    @Override void upsertCF(CqlSession session) {
        session.execute(QueryUtil.upsertCFQuery(config.getColsPerRow(), keyspaceName, tableName));
    }

    @Override public String readSingle(String key) throws Exception{
        int nRows = 0;

        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("key", key);
        bStmt.setConsistencyLevel(consistencyLevel(config.getReadConsistencyLevel()));
        ResultSet rs = session.execute(bStmt);
        List<Row> result=rs.all();

        if (!result.isEmpty())
        {
            nRows = result.size();
            if (config.getValidateRowsPerPartition() && nRows < (config.getRowsPerPartition()))
            {
                throw new Exception("Num rows returned not ok " + nRows);
            }

            if (ndbConfig.isValidateChecksum())
            {
                for (Row row : result)
                {
                    for (int i = 0; i < config.getColsPerRow(); i++)
                    {
                        String value = row.getString(getValueColumnName(i));
                        if (!CheckSumUtil.isChecksumValid(value))
                        {
                            throw new Exception(String.format("Value %s is corrupt. Key %s.", value, key));
                        }
                    }
                }
            }
        }
        else {
            return CacheMiss;
        }

        return ResultOK;
    }

    private String getValueColumnName(int index)
    {
        return "value" + index;
    }

    private ConsistencyLevel consistencyLevel(String consistencyLevelConfig) {
        int code = new DefaultConsistencyLevelRegistry().nameToCode(consistencyLevelConfig);
        return DefaultConsistencyLevel.fromCode(code);
    }

    @Override public String writeSingle(String key) {
        if(config.getRowsPerPartition() > 1)
        {
            if (config.getUseBatchWrites()) {
                BatchStatementBuilder builder = BatchStatement
                        .builder(BatchType.UNLOGGED)
                        .setConsistencyLevel(consistencyLevel(config.getWriteConsistencyLevel()));
                for (int i = 0; i < config.getRowsPerPartition(); i++) {
                    builder.addStatement(getWriteBStmt(key, i));
                }
                session.execute(builder.build());
            } else {
                session.execute(getWriteBStmt(key, dataGenerator.getRandomInteger() % config.getRowsPerPartition())
                        .setConsistencyLevel(consistencyLevel(config.getWriteConsistencyLevel())));

            }
        }
        else
        {
            session.execute(getWriteBStmt(key, 1)
                    .setConsistencyLevel(consistencyLevel(config.getWriteConsistencyLevel())));
        }
        return ResultOK;
    }

    private BoundStatement getWriteBStmt(String key, int col)
    {
        BoundStatement bStmt = writePstmt.bind();
        bStmt.setString("key", key);
        bStmt.setInt("column1", col);
        for (int i = 0; i < config.getColsPerRow(); i++)
        {
            bStmt.setString(getValueColumnName(i), this.dataGenerator.getRandomValue());
        }
        return bStmt;
    }

    @Override public void shutdown() throws Exception {
        session.close();
    }

    @Override public String getConnectionInfo() {
        int bytesPerCol=ndbConfig.getDataSize();
        int numColsPerRow=config.getColsPerRow();
        int numRowsPerPartition=config.getRowsPerPartition();
        int numPartitions= ndbConfig.getNumKeys();
        int RF = 3;
        Long numNodes = session.getMetadata().getNodes().values()
                .stream()
                .collect(groupingBy(Node::getDatacenter,counting()))
                .values().stream().findFirst().get();


        int partitionSizeInBytes = bytesPerCol * numColsPerRow * numRowsPerPartition;
        long totalSizeInBytes = (long) partitionSizeInBytes * numPartitions * RF;
        long totalSizeInBytesPerNode = totalSizeInBytes / numNodes;



        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Name - %s ::: ReadCL - %s : WriteCL - %s ::: " +
                        "DataSize per Node: ~[%s], Total DataSize on Cluster: ~[%s], Num nodes in C* DC: %s, PartitionSize: %s",
                clusterName, keyspaceName, tableName, config.getReadConsistencyLevel(), config.getWriteConsistencyLevel(),
                humanReadableByteCount(totalSizeInBytesPerNode),
                humanReadableByteCount(totalSizeInBytes),
                numNodes,
                humanReadableByteCount(partitionSizeInBytes));
    }

    private SimpleStatement SimpleStatement(String query) {
        return SimpleStatement.builder(query).build();
    }
}
