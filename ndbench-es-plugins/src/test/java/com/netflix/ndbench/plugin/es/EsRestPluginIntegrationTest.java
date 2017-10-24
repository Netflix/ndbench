package com.netflix.ndbench.plugin.es;

import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Verifies behavior of ES REST plugin by bringing up Elastic search in a docker container and operating against that
 * instance..
 */
@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({})
public class EsRestPluginIntegrationTest extends AbstractPluginIntegrationTest {

    static final String ELASTICSEARCH = "elasticsearch";


    private static final Logger logger = LoggerFactory.getLogger(EsRestPluginIntegrationTest.class);
    private static final String ES_HOST_PORT = "http://localhost:9200";


    /**
     * This check is required because DockerComposeRule .waitingForService() checks don't seem to wait for
     * connections to be establishable on port 9200. There is probably some way
     * to configure this, but this quick and dirty test does the job for now.
     * <p>
     * Note: This is similar to check code in test for transport protocol: could be refactored.
     */
    private static FutureTask<Boolean> checkEsServerUpTask = new FutureTask<Boolean>(new Callable<Boolean>() {


        @Override
        public Boolean call() throws Exception {
            EsRestPlugin plugin = getPlugin("localhost", "test_index_name", false, 0, 9200);
            while (!checkEsServerUpTask.isCancelled()) {
                logger.info("Checking if we can connect to elasticsearch (IGNORE EXCEPTIONS, PLEASE)");
                try {
                    if (StringUtils.isNotEmpty(EsUtils.httpGet(plugin.getEsRestEndpoint()))) {
                        logger.info("connection to elasticsearch succeeded !");
                        return true;           // success -- break out of loop
                    }
                } catch (Exception ignored) {
                }
                Thread.sleep(100);  // yes. it is an extra 1/10th of a second in happy path. but less code this way.
            }
            return false;
        }
    });


    @BeforeClass
    public static void initialize() throws Exception {
        if (disableIfDockerComposeUnavailable) {
            return;
        }
        if (StringUtils.isNotEmpty(System.getenv("ES_NDBENCH_NO_DOCKER"))) {
            return;
        }

        ExecutorService execSvc = Executors.newSingleThreadExecutor();
        execSvc.submit(checkEsServerUpTask);
        checkEsServerUpTask.get(40, TimeUnit.SECONDS);
        execSvc.shutdownNow();

        checkEsServerUpTask.cancel(true);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (disableIfDockerComposeUnavailable) {
            return;
        }
        if (StringUtils.isNotEmpty(System.getenv("ES_NDBENCH_NO_DOCKER"))) {
            return;
        }
        docker.containers().container(ELASTICSEARCH).stop();
    }


    @Test
    public void testCanReadWhatWeJustWrote() throws Exception {
        if (disableIfDockerComposeUnavailable) {
            return
                    ;
        }
        testCanReadWhatWeJustWroteUsingPlugin(
                getPlugin(/* specify host and avoid discovery mechanism */"localhost",
                        "test_index_name",
                        false, 0, 9200));
        testCanReadWhatWeJustWroteUsingPlugin(
                getPlugin(/* specify host and avoid discovery mechanism */"localhost",
                        "test_index_name",
                        true, 0, 9200));
        testCanReadWhatWeJustWroteUsingPlugin(
                getPlugin(/* force use of discovery mechanism */null,
                        "test_index_name",
                        false, 0, 9200));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIndexRollSettingsGreaterThan1440() throws Exception {
        EsRestPlugin plugin = getPlugin(/* specify host and avoid discovery mechanism */"localhost",
                "test_index_name",
                false, 1441, 9200);
        plugin.init(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIndexRollSettingsLessThan0() throws Exception {
        EsRestPlugin plugin = getPlugin(/* specify host and avoid discovery mechanism */"localhost",
                "test_index_name",
                false, -1, 9200);
        plugin.init(null);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIndexRollSettingsDoesntEvenlyDivide1440() throws Exception {
        EsRestPlugin plugin = getPlugin(/* specify host and avoid discovery mechanism */"localhost",
                "test_index_name",
                false, 7, 9200);
        plugin.init(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHttpsAndPortSettings() throws Exception {
        EsRestPlugin plugin = getPlugin(/* specify host and avoid discovery mechanism */"localhost",
                "test_index_name",
                false, 0, 443);
        plugin.init(null);
    }


    private void testCanReadWhatWeJustWroteUsingPlugin(EsRestPlugin plugin) throws Exception {
        String writeResult = plugin.writeSingle("the-key").toString();
        assert writeResult.contains("numRejectedExecutionExceptions=0");

        assert plugin.readSingle("the-key").equals(EsRestPlugin.RESULT_OK);
    }

}

