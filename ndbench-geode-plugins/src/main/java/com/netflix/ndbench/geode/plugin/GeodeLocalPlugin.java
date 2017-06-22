package com.netflix.ndbench.geode.plugin;

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
 * This is a Geode plugin which assumes that Geode processes
 * are running on the localhost.
 *
 * Locator process is running on localhost ip *127.0.0.1* and port is *55221*.
 * Also assumes a region with the name "Sample" name exists on
 * cache.
 *
 * @author Pulkit Chandra
 *
 */

@Singleton
@NdBenchClientPlugin("GeodeLocalhostClient")
//@Import(SpringDataGeodeConfiguration.class)

public class GeodeLocalPlugin implements NdBenchClient{

  private final static Logger logger = LoggerFactory.getLogger(GeodeLocalPlugin.class);

  private static final String ResultOK = "Ok";
  private static final String CacheMiss = null;

  private DataGenerator dataGenerator;

//  @Autowired
  private ClientCache clientCache;

//  @Autowired
  private Region<String, String> sampleRegion;

  public void init(final DataGenerator dataGenerator) throws Exception {
    this.dataGenerator = dataGenerator;
    logger.info("Initializing Geode Region");
    clientCache = new ClientCacheFactory()
        .addPoolLocator("127.0.0.1",55221)
        .create();
    sampleRegion =
        clientCache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY).create("Sample");
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
