/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.configs.CassandraGenericConfiguration;

import java.time.Instant;
import java.util.List;
import java.util.Random;
/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("CassJavaDriverBatch")
public class CassJavaDriverBatch extends CJavaDriverBasePlugin<CassandraGenericConfiguration> {

    //Settings
    private volatile String TableName2;

    Random randomObj = new Random();

    protected PreparedStatement writePstmt2;

    @Inject
    public CassJavaDriverBatch(CassJavaDriverManager javaDriverManager, CassandraGenericConfiguration cassConfigs) {
        super(javaDriverManager, cassConfigs);
    }

    @Override
    void prepStatements(Session session) {

        readPstmt = session.prepare(" SELECT cyclist_name, expense_id, amount, description, paid FROM " + tableName + " WHERE cyclist_name = ?" );
        writePstmt = session.prepare("INSERT INTO " + tableName + " (cyclist_name, expense_id, amount, description, paid) VALUES (?, ?, ?, ?, ?)");
        writePstmt2 = session.prepare("INSERT INTO "+TableName2+" (expense_id, cyclist_name) VALUES (?, ?)");
    }

    @Override
    void upsertKeyspace(Session session) {
        upsertGenereicKeyspace(session);
    }

    @Override
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (cyclist_name text, balance float STATIC, expense_id int, amount float, description text, paid boolean, PRIMARY KEY (cyclist_name, expense_id) ) WITH compression = {'sstable_compression': ''}");
        session.execute("CREATE TABLE IF NOT EXISTS "+TableName2+" (expense_id int, cyclist_name text, PRIMARY KEY (expense_id, cyclist_name)) WITH compression = {'sstable_compression': ''}");
    }

    @Override
    void preInit() {
        this.TableName2 = config.getCfname2();
    }

    @Override
    public String readSingle(String key) throws Exception {
        BoundStatement statement = readPstmt.bind();
        statement.setString("cyclist_name", key);
        statement.setConsistencyLevel(ConsistencyLevel.valueOf(config.getReadConsistencyLevel()));
        ResultSet rs = session.execute(statement);

        List<Row> result = rs.all();

        if (!result.isEmpty()) {

            if (result.size() < 1) {
                throw new Exception("Expecting non zero rows for cyclist_name: " + key);
            }
        } else {
            return CacheMiss;
        }

        return ResultOK;
    }

    @Override
    public String writeSingle(String key) throws Exception {

        BatchStatement batch = new BatchStatement();
        for (int i = 0; i < config.getBatchSize(); i++) {

            BoundStatement bStmt;
            if(config.getUseMultiPartition())
            {
                if(randomObj.nextBoolean())
                {
                 bStmt = getBStmtTable1(key);
                }
                else
                {
                    bStmt = getBStmtTable2(key);
                }
            }
            else
            {
                bStmt = getBStmtTable1(key);
            }

            bStmt.setConsistencyLevel(ConsistencyLevel.valueOf(config.getWriteConsistencyLevel()));
            batch.add(bStmt);
        }
        if(config.getUseTimestamp()) {
            batch.setDefaultTimestamp(Instant.now().toEpochMilli()*1000);
        }
        session.execute(batch);
        batch.clear();
        return ResultOK;
    }

    private BoundStatement getBStmtTable1(String key) {
        BoundStatement bStmt = writePstmt.bind();
        bStmt.setString("cyclist_name", key);
        bStmt.setInt("expense_id", this.dataGenerator.getRandomIntegerValue());
        bStmt.setFloat("amount", randomObj.nextFloat());
        bStmt.setString("description", this.dataGenerator.getRandomValue());
        bStmt.setBool("paid", randomObj.nextBoolean());
        return bStmt;
    }

    private BoundStatement getBStmtTable2(String key)
    {
        BoundStatement bStmt = writePstmt.bind();
        bStmt.setInt("expense_id", this.dataGenerator.getRandomIntegerValue());
        bStmt.setString("cyclist_name", key);
        return bStmt;
    }

}
