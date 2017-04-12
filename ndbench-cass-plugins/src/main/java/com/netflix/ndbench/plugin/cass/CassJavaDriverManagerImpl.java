/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.netflix.ndbench.plugin.cass.CassJavaDriverManager;

/**
 * @author vchella
 */
public class CassJavaDriverManagerImpl implements CassJavaDriverManager {

    Cluster cluster;
    Session session;

    @Override
    public Cluster registerCluster(String clName, String contactPoint) {
        cluster = Cluster.builder()
                .withClusterName(clName)
                .addContactPoint(contactPoint)
                .build();
        return cluster;
    }

    @Override
    public Session getSession(Cluster cluster) {
         session = cluster.connect();
         return session;
    }

    @Override
    public void shutDown() {
        cluster.close();
    }
}
