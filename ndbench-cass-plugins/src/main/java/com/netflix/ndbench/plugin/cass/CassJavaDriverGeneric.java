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


import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


@Singleton
@NdBenchClientPlugin("CassJavaDriverGeneric")
@SuppressWarnings("unused")
public class CassJavaDriverGeneric extends CJavaDriverBasePlugin {

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(CassJavaDriverGeneric.class);


    @Inject
    public CassJavaDriverGeneric(PropertyFactory propertyFactory, CassJavaDriverManager cassJavaDriverManager) {
        super(propertyFactory, cassJavaDriverManager);
    }

    @Override
    public String readSingle(String key) throws Exception {

        boolean success = true;
        int nCols = 0;

        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("key", key);
        bStmt.setConsistencyLevel(this.ReadConsistencyLevel);
        ResultSet rs = session.execute(bStmt);
        List<Row> result=rs.all();

        if (!result.isEmpty())
        {
            nCols = result.size();
            if (nCols < (this.MaxColCount)) {
                throw new Exception("Num Cols returned not ok " + nCols);
            }
        }
        else {
            return CacheMiss;
        }

        return ResultOK;
    }

    @Override
    public String writeSingle(String key) throws Exception {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (int i = 0; i < this.MaxColCount; i++) {
            BoundStatement bStmt = writePstmt.bind();
            bStmt.setString("key", key);
            bStmt.setInt("column1", i);
            bStmt.setString("value", this.dataGenerator.getRandomValue());
            batch.add(bStmt);
        }
    batch.setConsistencyLevel(this.WriteConsistencyLevel);
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

    @Override
    void upsertKeyspace(Session session) {
       upsertGenereicKeyspace();
    }
    @Override
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS "+TableName+" (key text, column1 int, value text, PRIMARY KEY ((key), column1))");

    }

    @Override
    void preInit() {

    }

    @Override
    void postInit() {

    }


    @Override
    void prepStatements(Session session) {
        writePstmt = session.prepare("INSERT INTO "+TableName+" (key, column1 , value ) VALUES (?, ?, ? )");
        readPstmt = session.prepare("Select * From "+TableName+" Where key = ?");
    }

}
