package com.netflix.ndbench.plugin.geode;


import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

import java.util.Properties;

/**
 * Created by Pulkit Chandra
 */
@SuppressWarnings("unused")
public class ClientAuthInitialize implements AuthInitialize {

  public static final String USER_NAME = "security-username";
  public static final String PASSWORD = "security-password";

  public static AuthInitialize create() {
    return new ClientAuthInitialize();
  }

  @Override
  public void close() {
  }

  @Override
  public Properties getCredentials(Properties arg0, DistributedMember arg1,
                                   boolean arg2) throws AuthenticationFailedException {
    Properties props = new Properties();
    props.put(USER_NAME, arg0.getProperty(USER_NAME));
    props.put(PASSWORD, arg0.getProperty(PASSWORD));
    return props;
  }

  @Override
  public void init(LogWriter arg0, LogWriter arg1)
      throws AuthenticationFailedException {
  }
}