package com.netflix.ndbench.plugin.datastax4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.inject.ImplementedBy;

@ImplementedBy(CassJavaDriverManagerImpl.class)
public interface CassJavaDriverManager {

    CqlSession getSession(String sessionName, String contactPoint, int connections, int port, String username, String password);
    CqlSession getSession(String sessionName, String contactPoint, int connections, int port);
}
