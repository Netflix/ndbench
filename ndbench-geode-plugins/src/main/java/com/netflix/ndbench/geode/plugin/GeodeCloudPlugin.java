package com.netflix.ndbench.geode.plugin;

import java.net.URI;
import java.util.List;
import java.util.Properties;

import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final static Logger logger = LoggerFactory.getLogger(GeodeLocalPlugin.class);

  private static final String ResultOK = "Ok";
  private static final String CacheMiss = null;

  private DataGenerator dataGenerator;

  private ClientCache clientCache;

  private Region<String, String> sampleRegion;
  @Override
  public void init(final DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;
        logger.info("Initializing Geode Region");
        Properties props = new Properties();
        props.setProperty("security-client-auth-init", "com.netflix.ndbench.geode.plugin.ClientAuthInitialize.create");

        ClientCacheFactory ccf = new ClientCacheFactory(props);
        List<URI> locatorList = EnvParser.getInstance().getLocators();
        for (URI locator : locatorList) {
          ccf.addPoolLocator(locator.getHost(), locator.getPort());
        }
        clientCache = ccf.create();
        sampleRegion = clientCache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY).create("ndbench");
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
