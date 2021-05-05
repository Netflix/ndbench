package com.netflix.ndbench.plugin.cass4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

public class CassJavaDriverManagerImpl implements CassJavaDriverManager {

    @Override public CqlSession getSession(String cName, String contactPoint, int connections, int port) {
        return getSession(cName, contactPoint, connections, port,  null, null);
    }

    @Override
    public CqlSession getSession(
            String cName, String contactPoint, int connections, int port, String username,
            String password) {
        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.SESSION_NAME, cName)
                .withString(DefaultDriverOption.CONTACT_POINTS, contactPoint + ":" + port)
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, connections)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, connections)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 32768)
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, username)
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, password)
                .build();
        return CqlSession.builder().withConfigLoader(configLoader).build();
    }

}
