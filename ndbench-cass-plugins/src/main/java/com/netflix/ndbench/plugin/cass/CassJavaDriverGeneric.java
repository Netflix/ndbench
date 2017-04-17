package com.netflix.ndbench.plugin.cass;


import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.slf4j.LoggerFactory;

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
        BatchStatement batch = new BatchStatement();
        for (int i = 0; i < this.MaxColCount; i++) {
            BoundStatement bStmt = writePstmt.bind();
            bStmt.setString("key", key);
            bStmt.setInt("column1", i);
            bStmt.setString("value", this.dataGenerator.getRandomValue());
            bStmt.setConsistencyLevel(this.WriteConsistencyLevel);
            batch.add(bStmt);
        }
        session.execute(batch);
        batch.clear();
        return ResultOK;
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
