package com.netflix.ndbench.plugin.geode;

import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 *
 * This class creates Geode plugin which talks to Geode cluster run
 * using BOSH.
 *
 * The advantage of using BOSH to run a Geode cluster is that it can be
 * run on any cloud e.g. AWS, GCP, vSphere etc without any change to the
 * actual Geode cluster.
 *
 * This Plugin also assumes that you are running NDBench on CloudFoundry,
 * which exposes the Geode cluster to any application(e.g. NDBench) using
 *  <b>VCAP_SERVICES</b> environment variable. We will parse that Environment
 * variable and connect to the Geode cluster.
 *
 *
 * @author Pulkit Chandra
 */

@Singleton
@NdBenchClientPlugin("GeodeCloudPlugin")
public class GeodeCloudPlugin implements NdBenchClient{

  private static final Logger logger = LoggerFactory.getLogger(GeodeCloudPlugin.class);
  private final PropertyFactory propertyFactory;

  private static final String ResultOK = "Ok";
  private static final String REGION = "ndbench";
  private static final String USER_NAME = "security-username";
  private static final String PASSWORD = "security-password";

  private static final String CacheMiss = null;
  private EnvParser envParser;

  private DataGenerator dataGenerator;

  private ClientCache clientCache;

  private Region<String, String> sampleRegion;


  @Inject
  public GeodeCloudPlugin(PropertyFactory propertyFactory, EnvParser envParser){
      this.propertyFactory = propertyFactory;
      this.envParser = envParser;
  }

  @Override
  public void init(final DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        logger.info("Initializing Geode Region");
        EnvParser envParser = this.envParser;
        if(this.propertyFactory.getProperty(NdBenchConstants.DISCOVERY_ENV).asString("").get().equals(NdBenchConstants.DISCOVERY_ENV_CF)) {
            Properties props = new Properties();
            props.setProperty(USER_NAME, envParser.getUsername());
            props.setProperty(PASSWORD, envParser.getPasssword());
            props.setProperty("security-client-auth-init", "com.netflix.ndbench.geode.plugin.ClientAuthInitialize.create");

            ClientCacheFactory ccf = new ClientCacheFactory(props);
            List<URI> locatorList = envParser.getLocators();
            for (URI locator : locatorList) {
                ccf.addPoolLocator(locator.getHost(), locator.getPort());
            }
            clientCache = ccf.create();
        }else{
            clientCache = new ClientCacheFactory()
                    .addPoolLocator("127.0.0.1",55221)
                    .create();
        }
        sampleRegion = clientCache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION);
  }

  public String readSingle(final String key) throws Exception {
    String result = sampleRegion.get(key);
    if (null != result){
      if (result.isEmpty()){
        throw new Exception("Data retrieved is not NULL but empty string ! ");
      }
    }
    else {
      return CacheMiss;
    }
    return ResultOK;
  }

  public String writeSingle(final String key) throws Exception {
    String result = sampleRegion.put(key, dataGenerator.getRandomValue());

    return result;
  }

    /**
     * Perform a bulk read operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> readBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

    /**
     * Perform a bulk write operation
     * @return a list of response codes
     * @throws Exception
     */
    public List<String> writeBulk(final List<String> keys) throws Exception {
        throw new UnsupportedOperationException("bulk operation is not supported");
    }

  public void shutdown() throws Exception {
    if (!clientCache.isClosed()){
      clientCache.close();
    }
  }

  public String getConnectionInfo() throws Exception {
    return clientCache.getDefaultPool().getLocators().toString();
  }

  public String runWorkFlow() throws Exception {
    return null;
  }
}
