/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

import java.time.Instant;
import java.util.List;
import java.util.Random;
/**
 * @author vchella
 */
@Singleton
@NdBenchClientPlugin("CassJavaDriverBatch")
public class CassJavaDriverBatch extends CJavaDriverBasePlugin {

    //Settings
    private static String TableName2;
    private static Integer batchSize;
    private static Boolean useTimeStamp;
    private static Boolean useMultiPartition;

    Random randomObj = new Random();

    protected PreparedStatement writePstmt2;

    @Inject
    public CassJavaDriverBatch(PropertyFactory propertyFactory, CassJavaDriverManager javaDriverManager) {
        super(propertyFactory, javaDriverManager);
    }

    @Override
    void prepStatements(Session session) {

        readPstmt = session.prepare(" SELECT cyclist_name, expense_id, amount, description, paid FROM "+TableName+" WHERE cyclist_name = ?" );

        writePstmt = session.prepare("INSERT INTO "+TableName+" (cyclist_name, expense_id, amount, description, paid) VALUES (?, ?, ?, ?, ?)");
        writePstmt2 = session.prepare("INSERT INTO "+TableName2+" (expense_id, cyclist_name) VALUES (?, ?)");


    }

    @Override
    void upsertKeyspace(Session session) {
        upsertGenereicKeyspace();
    }

    @Override
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS "+TableName+" (cyclist_name text, balance float STATIC, expense_id int, amount float, description text, paid boolean, PRIMARY KEY (cyclist_name, expense_id) )");
        session.execute("CREATE TABLE IF NOT EXISTS "+TableName2+" (expense_id int, cyclist_name text, PRIMARY KEY (expense_id, cyclist_name)) ");
    }

    @Override
    void preInit() {
        TableName2  = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.cfname2").asString("test2").get();
        batchSize = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.batchSize").asInteger(3).get();
        useTimeStamp = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.useTimestamp").asBoolean(true).get();
        useMultiPartition = propertyFactory.getProperty(NdBenchConstants.PROP_NAMESPACE +"cass.useMultiPartition").asBoolean(false).get();
    }

    @Override
    void postInit() {

    }

    @Override
    public String readSingle(String key) throws Exception {
        BoundStatement statement = readPstmt.bind();
        statement.setString("cyclist_name", key);
        statement.setConsistencyLevel(this.ReadConsistencyLevel);
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
        for (int i = 0; i < this.batchSize; i++) {

            BoundStatement bStmt;
            if(useMultiPartition)
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

            bStmt.setConsistencyLevel(this.WriteConsistencyLevel);
            batch.add(bStmt);
        }
        if(useTimeStamp) {
            batch.setDefaultTimestamp(Instant.now().toEpochMilli()*1000);
        }
        session.execute(batch);
        batch.clear();
        return ResultOK;
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
