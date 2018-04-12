package com.netflix.ndbench.geode.plugin;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.ndbench.plugin.geode.ClientAuthInitialize;

import java.util.Properties;


public class ClientAuthInitializeTest {

  @Test
  public void getCredentialsTest() {
    ClientAuthInitialize clientAuthInitialize = new ClientAuthInitialize();

    Properties inputProperties = new Properties();
    inputProperties.setProperty("security-username", "user");
    inputProperties.setProperty("security-password", "password");

    Properties resultProperties = clientAuthInitialize.getCredentials(inputProperties, null, false);

    Assert.assertEquals(resultProperties, inputProperties);
  }

}
