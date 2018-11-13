/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass.astyanax;

import com.google.inject.ImplementedBy;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * @author vchella
 */
@ImplementedBy(CassA6XManagerImpl.class)
public interface CassA6XManager
{
    Cluster registerCluster(String clName, String contactPoint, int port) throws ConnectionException;
    Keyspace registerKeyspace(String clusterName, String ksName, String contactPoint, int port) throws ConnectionException;
    void shutDown() throws Exception;
}
