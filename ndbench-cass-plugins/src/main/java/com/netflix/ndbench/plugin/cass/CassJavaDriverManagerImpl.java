/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.NettySSLOptions;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslContext;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;


/**
 * @author vchella
 */
public class CassJavaDriverManagerImpl implements CassJavaDriverManager {

    Cluster cluster;
    Session session;

    @Override
    public Cluster registerCluster(String clName, String contactPoint, int connections, int port) {
        return registerCluster(clName,contactPoint,connections,port,null,null,null,null);
    }
        @Override
    public Cluster registerCluster(String clName, String contactPoint, int connections, int port, String username, String password, String truststorePath, String truststorePass) {
    
        PoolingOptions poolingOpts = new PoolingOptions()
                                     .setConnectionsPerHost(HostDistance.LOCAL, connections, connections)
                                     .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768);

        KeyStore ks = KeyStore.getInstance("JKS");
        InputStream trustStore = new java.io.FileInputStream(truststorePath);
        ks.load(trustStore, truststorePass.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        SslContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
        sslContext.init(null,tmf.getTrustManagers,null);

        SSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();

        Cluster.Builder clusterBuilder = Cluster.builder()
                .withClusterName(clName)
                .addContactPoint(contactPoint)
                .withPoolingOptions(poolingOpts)
                .withPort(port)
                .withLoadBalancingPolicy( new TokenAwarePolicy( new RoundRobinPolicy() ) );
        if ((username != null) && (password != null)) {
            clusterBuilder = clusterBuilder.withCredentials(username, password);
        }

        if ((truststorePath !=null)) {
            clusterBuilder = clusterBuilder.withSSL(sslOptions);
        }

        cluster = clusterBuilder.build();
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
