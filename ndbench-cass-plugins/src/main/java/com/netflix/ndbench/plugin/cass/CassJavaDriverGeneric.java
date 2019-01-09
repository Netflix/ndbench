/*
 *  Copyright 2018 Netflix, Inc.
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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.plugin.configs.CassandraGenericConfiguration;

import static com.netflix.ndbench.core.util.NdbUtil.humanReadableByteCount;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

@Singleton
@NdBenchClientPlugin("CassJavaDriverGeneric")
@SuppressWarnings("unused")
public class CassJavaDriverGeneric extends CJavaDriverBasePlugin<CassandraGenericConfiguration> {
    private static final Logger logger = LoggerFactory.getLogger(CassJavaDriverGeneric.class);

    @Inject
    public CassJavaDriverGeneric(CassJavaDriverManager cassJavaDriverManager, IConfiguration coreConfig, CassandraGenericConfiguration cassConfigs) {
        super(cassJavaDriverManager, coreConfig, cassConfigs);
    }

    @Override
    public String readSingle(String key) throws Exception {

        boolean success = true;
        int nRows = 0;

        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("key", key);
        bStmt.setConsistencyLevel(ConsistencyLevel.valueOf(config.getReadConsistencyLevel()));
        ResultSet rs = session.execute(bStmt);
        List<Row> result=rs.all();

        if (!result.isEmpty())
        {
            nRows = result.size();
            if (nRows < (config.getRowsPerPartition())) {
                throw new Exception("Num rows returned not ok " + nRows);
            }
        }
        else {
            return CacheMiss;
        }

        return ResultOK;
    }

    @Override
    public String writeSingle(String key)
    {
        if(config.getRowsPerPartition() > 1)
        {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batch.setConsistencyLevel(ConsistencyLevel.valueOf(config.getWriteConsistencyLevel()));
            for (int i = 0; i < config.getRowsPerPartition(); i++)
            {
                batch.add(getWriteBStmt(key,i));
            }
            session.execute(batch);
            batch.clear();
        }
        else
        {
            session.execute(getWriteBStmt(key, 1)
                            .setConsistencyLevel(ConsistencyLevel.valueOf(config.getWriteConsistencyLevel())));
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
            bStmt.setString("value"+i, this.dataGenerator.getRandomValue());
        }
        return bStmt;
    }

    @Override
    void upsertKeyspace(Session session) {
       upsertGenereicKeyspace(session);
    }

    @Override
    void upsertCF(Session session) {
        String createTblQuery = "CREATE TABLE IF NOT EXISTS %s.%s (key text, column1 int, %s, PRIMARY KEY ((key), column1)) WITH compression = {'sstable_compression': ''}";

        String values = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "value"+i+" text").collect(Collectors.joining(", "));
        session.execute(String.format(createTblQuery, keyspaceName, tableName, values));

    }

    @Override
    void prepStatements(Session session) {

        String insertQuery = "INSERT INTO %s.%s (key, column1 , %s ) VALUES (?, ?, %s )";
        int nCols = config.getColsPerRow();

        String values = IntStream.range(0, nCols).mapToObj(i -> "value"+i).collect(Collectors.joining(", "));
        String bindValues = IntStream.range(0, nCols).mapToObj(i -> "?").collect(Collectors.joining(", "));

        writePstmt = session.prepare(String.format(insertQuery, keyspaceName, tableName, values, bindValues));
        readPstmt = session.prepare(String.format("SELECT * FROM %s.%s WHERE key = ?", keyspaceName, tableName));
    }

    @Override
    public String getConnectionInfo() {
        int bytesPerCol=coreConfig.getDataSize();
        int numColsPerRow=config.getColsPerRow();
        int numRowsPerPartition=config.getRowsPerPartition();
        int numPartitions= coreConfig.getNumKeys();
        int RF = 3;
        Long numNodes = cluster.getMetadata().getAllHosts()
                               .stream()
                               .collect(groupingBy(Host::getDatacenter,counting()))
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
}
