package com.netflix.ndbench.geode.plugin;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyContainer;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.plugin.geode.EnvParser;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.mockito.Mockito.*;


public class EnvParserTest {

  PropertyFactory propertyFactory = mock(PropertyFactory.class);
  EnvParser envParser;

  @Before
  public void setUp(){
    envParser = new EnvParser(propertyFactory);
  }

  @Test
  public void getLocatorsReturnsAListOfValidLocatorsTest() throws Exception{
    String vcapServices = IOUtils.toString(
            this.getClass().getClassLoader().getResourceAsStream("vcap_services.json"), "UTF-8");
    this.setCreditentials(vcapServices);
    List<URI> results = envParser.getLocators();

    Assert.assertNotEquals(0, results.size());
    Assert.assertTrue(results.contains(new URI("locator://10.0.0.1:55221")));
    Assert.assertTrue(results.contains(new URI("locator://10.0.0.2:55221")));
    Assert.assertTrue(results.contains(new URI("locator://10.0.0.3:55221")));
  }

  @Test
  public void getLocatorsThrowsExceptionTest() throws Exception {
    String vcapServices = IOUtils.toString(
            this.getClass().getClassLoader().getResourceAsStream("bad_vcap_services.json"), "UTF-8");
    this.setCreditentials(vcapServices);

    boolean exceptionThrown = false;
    try{
     envParser.getLocators();
    }catch (Exception e){
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void getUsernameTest() throws Exception{
    String vcapServices = IOUtils.toString(
            this.getClass().getClassLoader().getResourceAsStream("vcap_services.json"), "UTF-8");
    this.setCreditentials(vcapServices);

    Assert.assertEquals("operator", envParser.getUsername());
  }

  @Test
  public void getPasswordTest() throws Exception{
    String vcapServices = IOUtils.toString(
            this.getClass().getClassLoader().getResourceAsStream("vcap_services.json"), "UTF-8");
    this.setCreditentials(vcapServices);

    Assert.assertEquals("Tuo6wkdq0gqcl2ty6t2bQ", envParser.getPasssword());
  }

  private void setCreditentials(String vcapServices) throws Exception{
    // Mocking the credentials for getCredentials
    PropertyContainer mockPropertyContainer = mock(PropertyContainer.class);
    Property mockProperty = mock(Property.class);
    when(mockPropertyContainer.asString(anyString())).thenReturn(mockProperty);
    when(mockProperty.get()).thenReturn(vcapServices);
    when(propertyFactory.getProperty(anyString())).thenReturn(mockPropertyContainer);
  }
}
