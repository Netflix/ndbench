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
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.configs.CassandraUdtConfiguration;
import org.apache.commons.lang.RandomStringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author vchella
 */
@NdBenchClientPlugin("CassUDTFrozen")
@SuppressWarnings("unused")
public class CassUDTFrozen extends CJavaDriverBasePlugin<CassandraUdtConfiguration> {

    //Settings - preInit
    private volatile String addressType;
    private volatile String fullnameType;
    private volatile String emailType;
    private volatile Boolean randomReads;
    private volatile Boolean randomWrites;

    //Settings postInit
    private volatile UserType cassFullnameType;
    private volatile UserType cassEmailType;
    private volatile UserType cassAddressType;

    private PreparedStatement readPstmt1;
    private PreparedStatement readPstmt2;
    private PreparedStatement readPstmt3;
    private PreparedStatement insertPstmt1;
    private PreparedStatement updatePstmt1;
    private PreparedStatement casPstmt1;
    private PreparedStatement casPstmt2;
    private PreparedStatement updatePstmt2;

    @Override
    void preInit() {
        addressType = config.getAddressType();
        fullnameType = config.getFullnameType();
        emailType = config.getEmailType();
        randomReads = config.getRandomReads();
        randomWrites = config.getRandomWrites();

    }
    @Inject
    public CassUDTFrozen(CassJavaDriverManager javaDriverManager,
                         CassandraUdtConfiguration cassUdtConfigs) {
        super(javaDriverManager, cassUdtConfigs);
    }

    @Override
    void upsertKeyspace(Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " +KeyspaceName+" WITH replication = {'class': 'NetworkTopologyStrategy','eu-west': '3','us-east': '3'};");
        session.execute("Use " + KeyspaceName);

    }

    @Override
    void upsertCF(Session session) {

        session.execute("CREATE TYPE  IF NOT EXISTS  "+ addressType +" (street text, city text, zip_code int, phones set<text>)");

        session.execute("CREATE TYPE  IF NOT EXISTS  "+ fullnameType +" (firstname text, lastname text)");

        session.execute("CREATE TYPE IF NOT EXISTS  "+ emailType +" (fp text, domain text)");

        session.execute("CREATE TABLE IF NOT EXISTS  "+TableName+" ( id text PRIMARY KEY, name frozen <fullname_type>, emails set<frozen <email_type>>, billing_addresses map<text, frozen <address_type>>, account_type text)");
    }

    @Override
    void prepStatements(Session session) {
        readPstmt1 = session.prepare(" SELECT * FROM "+TableName+" WHERE id = ?" );
        readPstmt2 = session.prepare(" SELECT name.lastname FROM "+TableName+" WHERE id = ?" );
        readPstmt3 = session.prepare(" SELECT emails, billing_addresses FROM "+TableName+" WHERE id = ?" );

        insertPstmt1 = session.prepare("INSERT INTO "+TableName+" (id, name, account_type) VALUES  (?, ?, ?)");


        updatePstmt1 = session.prepare("UPDATE "+TableName+" SET billing_addresses = billing_addresses + ? " +
                ", emails = emails + ? WHERE id = ?");


        casPstmt1 = session.prepare("UPDATE "+TableName+" SET billing_addresses = billing_addresses + ? WHERE id = ?                   if account_type='Paid'");

        casPstmt2 = session.prepare("UPDATE "+TableName+" SET emails = emails - ? WHERE id = ? if exists");

        updatePstmt2 = session.prepare("UPDATE "+TableName+" SET billing_addresses = billing_addresses - ? WHERE id = ?");

    }
    @Override
    void postInit() {
        cassAddressType = session.getCluster().
                getMetadata().getKeyspace(KeyspaceName).getUserType("address_type");
        cassFullnameType = session.getCluster().
                getMetadata().getKeyspace(KeyspaceName).getUserType("fullname_type");
        cassEmailType = session.getCluster().
                getMetadata().getKeyspace(KeyspaceName).getUserType("email_type");
    }

    @Override
    public String readSingle(String key) throws Exception {

        int option = -1;
        if(randomReads) {
            option = this.dataGenerator.getRandomInteger() % 3;
        }

        switch (option)
        {
            case 0:
                return readAllByKey(key);
            case 1:
                return readUDTByKey(key);
            case 2:
                return readCollectionsByKey(key);
            default:
                 if (readAllByKey(key) != CacheMiss && readUDTByKey(key) != CacheMiss && readCollectionsByKey(key) != CacheMiss)
                    return ResultOK;
                 return CacheMiss;
        }


    }

    private String readAllByKey(String key) {
        return readByKeyAndStatement(key, readPstmt1);
    }

    private String readByKeyAndStatement(String key, PreparedStatement preparedStatement)
    {
        BoundStatement statement = preparedStatement.bind();
        statement.setString("id", key);
        statement.setConsistencyLevel(this.ReadConsistencyLevel);
        ResultSet rs = session.execute(statement);

        List<Row> result = rs.all();

        if (result.isEmpty()) {
            return CacheMiss;
        }

        return ResultOK;
    }

    private String readUDTByKey(String key) {
        return readByKeyAndStatement(key, readPstmt2);
    }

    private String readCollectionsByKey(String key) {
        return readByKeyAndStatement(key, readPstmt3);
    }

    @Override
    public String writeSingle(String key) throws Exception {

        int option = -1;
        if(randomWrites) {
            option = this.dataGenerator.getRandomInteger() % 3;
        }
        switch (option)
        {
            case 0:
                return insertSimple(key);
            case 1:
                return updateSimple(key);
            case 2:
                return updateCasSimple(key);
            default:
                if (insertSimple(key) != CacheMiss && updateSimple(key) != CacheMiss && updateCasSimple(key) != CacheMiss)
                    return ResultOK;
                return CacheMiss;
        }




    }

    private String insertSimple(String key) {

        UDTValue name = cassFullnameType.newValue();
        name.setString("firstname", this.dataGenerator.getRandomValue());
        name.setString("lastname", this.dataGenerator.getRandomValue());

        BoundStatement bStmt = insertPstmt1.bind(key,name,this.dataGenerator.getRandomInteger()%2==0?"Paid":"Free");

        bStmt.setConsistencyLevel(this.WriteConsistencyLevel);

        ResultSet rs = session.execute(bStmt);

        if (rs !=null)
            return ResultOK;

        return ResutlFailed;
    }

    private String updateSimple(String key) {

        Map<String, UDTValue> billing_addresses = new HashMap<>();

        HashSet<String> phones = new HashSet<>();
        for (int i = 0; i < this.dataGenerator.getRandomInteger()%6+1; i++) {
            phones.add(RandomStringUtils.randomAlphanumeric(10));
        }


        UDTValue address = cassAddressType.newValue();
        address.setString("street", RandomStringUtils.randomAlphanumeric(30));
        address.setString("city", RandomStringUtils.randomAlphanumeric(20));
        address.setInt("zip_code", this.dataGenerator.getRandomInteger());
        address.setSet("phones", phones);

        billing_addresses.put(RandomStringUtils.randomAlphanumeric(8),address);


        HashSet<UDTValue> emails = new HashSet<>();
        for (int i = 0; i < this.dataGenerator.getRandomInteger()%5+1; i++) {
            UDTValue email = cassEmailType.newValue();
            email.setString("fp",RandomStringUtils.randomAlphanumeric(15));
            email.setString("domain",RandomStringUtils.randomAlphanumeric(10));
            emails.add(email);
        }

        BoundStatement bStmt = updatePstmt1.bind();

        bStmt.setMap("billing_addresses", billing_addresses);
        bStmt.setSet("emails", emails);
        bStmt.setString("id", key);

        bStmt.setConsistencyLevel(this.WriteConsistencyLevel);

        ResultSet rs = session.execute(bStmt);

        if (rs !=null)
            return ResultOK;

        return ResutlFailed;
    }


    private String updateCasSimple(String key) {

        BatchStatement batch = new BatchStatement();

        //Simple CAS stmt with billing_addresses '+' operation
        Map<String, UDTValue> billing_addresses = new HashMap<>();

        HashSet<String> phones = new HashSet<>();
        for (int i = 0; i < this.dataGenerator.getRandomInteger()%6+1; i++) {
            phones.add(RandomStringUtils.randomAlphanumeric(10));
        }


        UDTValue address = cassAddressType.newValue();
        address.setString("street", RandomStringUtils.randomAlphanumeric(30));
        address.setString("city", RandomStringUtils.randomAlphanumeric(20));
        address.setInt("zip_code", this.dataGenerator.getRandomInteger());
        address.setSet("phones", phones);

        billing_addresses.put(RandomStringUtils.randomAlphanumeric(8),address);

        BoundStatement bStmt1 = casPstmt1.bind();

        bStmt1.setMap("billing_addresses", billing_addresses);
        bStmt1.setString("id", key);
        bStmt1.setConsistencyLevel(this.WriteConsistencyLevel);

        batch.add(bStmt1);

        //Simple CAS stmt with email '-' operation
        HashSet<UDTValue> emails = new HashSet<>();
        for (int i = 0; i < this.dataGenerator.getRandomInteger()%2+1; i++) {
            UDTValue email = cassEmailType.newValue();
            email.setString("fp",RandomStringUtils.randomAlphanumeric(15));
            email.setString("domain",RandomStringUtils.randomAlphanumeric(10));
            emails.add(email);
        }


        BoundStatement bStmt2 = casPstmt2.bind();

        bStmt2.setSet("emails", emails);
        bStmt2.setString("id", key);
        bStmt2.setConsistencyLevel(this.WriteConsistencyLevel);

        batch.add(bStmt2);

        // Simple Update Stmt with `-` operation
//
//        Map<String, UDTValue> billing_addresses2 = new HashMap<>();
//
//        HashSet<String> phones2 = new HashSet<>();
//        for (int i = 0; i < this.dataGenerator.getRandomInteger()%6; i++) {
//            phones2.add(RandomStringUtils.randomAlphanumeric(10));
//        }
//
//
//        UDTValue address2 = addressType.newValue();
//        address2.setString("street", RandomStringUtils.randomAlphanumeric(30));
//        address2.setString("city", RandomStringUtils.randomAlphanumeric(20));
//        address2.setInt("zip_code", this.dataGenerator.getRandomInteger());
//        address2.setSet("phones", phones2);
//
//        billing_addresses2.put(RandomStringUtils.randomAlphanumeric(8),address);
//
//        BoundStatement bStmt3 = updatePstmt2.bind();
//
//        bStmt3.setMap("billing_addresses", billing_addresses2);
//
//        bStmt3.setString("id", key);
//
//        bStmt3.setConsistencyLevel(this.WriteConsistencyLevel);
//
//        batch.add(bStmt3);

        ResultSet rs =  session.execute(batch);

        batch.clear();

        if (rs !=null)
            return ResultOK;

        return ResutlFailed;
    }
    @Override
    public List<String> readBulk(List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }
    @Override
    public List<String> writeBulk(List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }
}
