package com.netflix.ndbench.geode.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reused from
 * https://github.com/gemfire/cf-gemfire-connector-examples
 *
 * @author Pulkit Chandra
 */
public class EnvParser {
  private final static Logger logger = LoggerFactory.getLogger(EnvParser.class);

  private static EnvParser instance;
  private final Pattern p = Pattern.compile("(.*)\\[(\\d*)\\]");

  private EnvParser() {
  }

  public static EnvParser getInstance() {
    if (instance != null) {
      return instance;
    }
    synchronized (EnvParser.class) {
      if (instance == null) {
        instance = new EnvParser();
      }
    }
    return instance;
  }

  public List<URI> getLocators() throws IOException, URISyntaxException {
    List<URI> locatorList = new ArrayList<URI>();
    Map credentials = getCredentials();
    List<String> locators = (List<String>) credentials.get("locators");
    for (String locator : locators) {
      Matcher m = p.matcher(locator);
      if (!m.matches()) {
        throw new IllegalStateException("Unexpected locator format. expected host[port], got"+locator);
      }
      locatorList.add(new URI("locator://" + m.group(1) + ":" + m.group(2)));
    }
    return locatorList;
  }

  public String getUsername() throws IOException {
    Map credentials = getCredentials();
    List<Map<String, String>> userData = (List<Map<String, String>>) credentials.get("users");
    return userData.get(0).get("username");
  }


  public String getPasssword() throws IOException {
    Map credentials = getCredentials();
    List<Map<String, String>> userData = (List<Map<String, String>>) credentials.get("users");
    return userData.get(0).get("password");
  }

  private Map getCredentials() throws IOException {
    Map credentials = null;
    String envContent = System.getenv().get("VCAP_SERVICES");
    ObjectMapper objectMapper = new ObjectMapper();
    Map services = objectMapper.readValue(envContent, Map.class);
    List gemfireService = getGemFireService(services);
    if (gemfireService != null) {
      Map serviceInstance = (Map) gemfireService.get(0);
      credentials = (Map) serviceInstance.get("credentials");
    }
    return credentials;

  }

  private List getGemFireService(Map services) {
    List l = (List) services.get("p-cloudcache");
    if (l == null) {
      throw new IllegalStateException("GemFire service is not bound to this application");
    }
    return l;
  }
}