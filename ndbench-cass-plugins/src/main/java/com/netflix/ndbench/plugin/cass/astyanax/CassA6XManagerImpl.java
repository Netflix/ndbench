/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass.astyanax;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * @author vchella
 */
@Singleton
public class CassA6XManagerImpl implements CassA6XManager
{
    private static final Logger logger = LoggerFactory.getLogger(CassA6XManagerImpl.class);
    private AstyanaxContext<Cluster> clusterContext;
    private AstyanaxContext<Keyspace> keyspaceContext;

    public synchronized Cluster registerCluster(String clusterName, String contactPoint, int port)
    {
        logger.info("Connected Cassandra Cluster: " + clusterName);
        if(clusterContext == null)
        {
            clusterContext = new AstyanaxContext.Builder().forCluster(clusterName)
                                                          .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                                                                     .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                                          )
                                                          .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                                                                           .setPort(port)
                                                                                           .setMaxConnsPerHost(1)
                                                                                           .setSeeds(contactPoint)
                                                          )
                                                          .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                                                          .buildCluster(ThriftFamilyFactory.getInstance());
            clusterContext.start();
        }
        logger.warn("Cluster already registered");
        return clusterContext.getClient();
    }

    public synchronized Keyspace registerKeyspace(String clusterName, String ksName, String contactPoint, int port)
    {
        logger.info("Cassandra  Cluster: " + clusterName);
        if(keyspaceContext == null)
        {
            keyspaceContext = new AstyanaxContext.Builder()
                              .forCluster(clusterName)
                              .forKeyspace(ksName)
                              .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                                         .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                              )
                              .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                                               .setPort(port)
                                                               .setMaxConnsPerHost(1)
                                                               .setSeeds(contactPoint)
                              )
                              .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                              .buildKeyspace(ThriftFamilyFactory.getInstance());
            keyspaceContext.start();
        }
        logger.warn("Keyspace already registered");
        return keyspaceContext.getClient();
    }

    public void shutDown()
    {
        this.keyspaceContext.shutdown();
        this.clusterContext.shutdown();
    }
}
