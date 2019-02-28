package com.netflix.ndbench.plugin.geode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.archaius.api.PropertyFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
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
@Singleton
public class EnvParser {
  private final String PROPERTIES_VCAP_SERVICES = "VCAP_SERVICES";
  private final String SERVICE_NAME = "p-cloudcache";
  private PropertyFactory propertyFactory;

  private final Pattern p = Pattern.compile("(.*)\\[(\\d*)\\]");

  @Inject
  public EnvParser(PropertyFactory propertyFactory) {
    this.propertyFactory = propertyFactory;

  }

  public List<URI> getLocators() throws IOException, URISyntaxException {
    List<URI> locatorList = new ArrayList<>();
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
    String envContent = propertyFactory.getProperty(PROPERTIES_VCAP_SERVICES).asString("{}").get();
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
    List l = (List) services.get(SERVICE_NAME);
    if (l == null) {
      throw new IllegalStateException("GemFire service is not bound to this application");
    }
    return l;
  }
}