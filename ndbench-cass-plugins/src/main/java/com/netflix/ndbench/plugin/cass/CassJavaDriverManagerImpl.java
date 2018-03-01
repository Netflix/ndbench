/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.policies.*;


/**
 * @author vchella
 */
public class CassJavaDriverManagerImpl implements CassJavaDriverManager {

    Cluster cluster;
    Session session;

    @Override
    public Cluster registerCluster(String clName, String contactPoint, int connections, int port) {
    
        PoolingOptions poolingOpts = new PoolingOptions()
                                     .setConnectionsPerHost(HostDistance.LOCAL, connections, connections)
                                     .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768);
    
        cluster = Cluster.builder()
                .withClusterName(clName)
                .addContactPoint(contactPoint)
                .withPoolingOptions(poolingOpts)
                .withPort(port)
                .withLoadBalancingPolicy( new TokenAwarePolicy( new RoundRobinPolicy() ) )
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
