package com.netflix.ndbench.plugin.cass4;

import com.datastax.oss.driver.api.core.CqlSession;

public interface CassJavaDriverManager {

    CqlSession getSession(String clName, String contactPoint, int connections, int port, String username, String password);
    CqlSession getSession(String clName, String contactPoint, int connections, int port);
}
