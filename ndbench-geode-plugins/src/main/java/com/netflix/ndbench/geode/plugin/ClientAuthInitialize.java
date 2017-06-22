package com.netflix.ndbench.geode.plugin;


import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Pulkit Chandra
 */
@SuppressWarnings("unused")
public class ClientAuthInitialize implements AuthInitialize {
  private EnvParser env = EnvParser.getInstance();

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
    try {
      String username = env.getUsername();
      String password = env.getPasssword();
      props.put(USER_NAME, username);
      props.put(PASSWORD, password);
    } catch (IOException e) {
      throw new AuthenticationFailedException("Exception reading username/password from env variables ", e);
    }
    return props;
  }

  @Override
  public void init(LogWriter arg0, LogWriter arg1)
      throws AuthenticationFailedException {
  }
}