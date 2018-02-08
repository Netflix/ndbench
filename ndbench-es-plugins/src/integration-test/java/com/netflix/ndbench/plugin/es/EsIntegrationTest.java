package com.netflix.ndbench.plugin.es;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/*
Verifies behavior of ES REST plugin by bringing up Elastic search in a docker container and operating against that
instance..

*Note: these tests runs within a Docker container.
*
 */
public class EsIntegrationTest extends AbstractPluginIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(EsIntegrationTest.class);

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
    public static void initialize() {
        if (disableDueToDockerExecutableUnavailability) {
            throw new IllegalStateException("ES Integration test runs within the Docker container, Docker was not detected in your environment");
        }
        if (StringUtils.isNotEmpty(System.getenv("ES_NDBENCH_NO_DOCKER"))) {
            throw new IllegalStateException("ES Integration test runs within the Docker container, Docker was not detected in your environment");
        }

        ExecutorService execSvc = Executors.newSingleThreadExecutor();
        execSvc.submit(checkEsServerUpTask);
        try {
            checkEsServerUpTask.get(40, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Exception encountered when checking if ES is up", e);
        }

        execSvc.shutdownNow();

        checkEsServerUpTask.cancel(true);

    }

    @AfterClass
    public static void tearDown() {
        try {
            if (docker != null) {
                docker.containers().container(ELASTICSEARCH).stop();
            }

        } catch (IOException e) {
            throw new RuntimeException("I/O exception when trying to stop Docker container.", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Trying to stop Docker container was unexpectedluy interrupted.", e);
        }
    }

    @Test
    public void testCanReadWhatWeJustWrote() throws Exception {
        if (disableDueToDockerExecutableUnavailability) {
            throw new IllegalStateException("ES Integration test runs within the Docker container, Docker was not detected in your environment");
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

    private void testCanReadWhatWeJustWroteUsingPlugin(EsRestPlugin plugin) throws Exception {
        String writeResult = plugin.writeSingle("the-key").toString();
        assert writeResult.contains("numRejectedExecutionExceptions=0");

        assert plugin.readSingle("the-key").equals(EsRestPlugin.RESULT_OK);
    }

}