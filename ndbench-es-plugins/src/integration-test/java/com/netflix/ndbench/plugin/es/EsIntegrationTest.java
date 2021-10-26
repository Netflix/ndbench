package com.netflix.ndbench.plugin.es;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies behavior of ES REST plugin by bringing up Elasticsearch in a docker container
 * and operating against that instance.
 * <p>
 * Note: these tests runs within a Docker container.
 */
public class EsIntegrationTest extends AbstractPluginIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(EsIntegrationTest.class);

    @BeforeClass
    public static void initialize() throws IOException {
        if (!isDockerAvailable) {
            throw new IllegalStateException("Docker was not detected in your environment");
        }
        if (StringUtils.isNotEmpty(System.getenv("ES_NDBENCH_NO_DOCKER"))) {
            throw new IllegalStateException("ES_NDBENCH_NO_DOCKER is configured to skip integration test");
        }

        String localRestEndpoint = "http://localhost:9200";
        String localRestResponse = EsUtils.httpGetWithRetries(
                "http://localhost:9200", 100, 600);

        if (StringUtils.isNotBlank(localRestResponse)) {
            logger.info("Connected to Elasticsearch at {}", localRestEndpoint);
        } else {
            throw new RuntimeException("Exception getting response from Elasticsearch");
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            if (docker != null) {
                docker.containers().container(ELASTICSEARCH).stop();
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Exception trying to stop Docker container", e);
        }
    }

    @Test
    public void testVerifyWrites() throws Exception {
        if (!isDockerAvailable) {
            throw new IllegalStateException("Docker was not detected in your environment");
        }

        testVerifySingleWrite(
                getPlugin("localhost",
                        "test_index_name",
                        false, 0, 9200));
        testVerifySingleWrite(
                getPlugin("localhost",
                        "test_index_name",
                        true, 0, 9200));
        testVerifySingleWrite(
                getPlugin(null,
                        "test_index_name",
                        false, 0, 9200));
    }

    private void testVerifySingleWrite(EsRestPlugin plugin) throws Exception {
        String writeResult = plugin.writeSingle("the-key").toString();

        assertTrue(writeResult.contains("numRejectedExecutionExceptions=0"));
        assertEquals(EsRestPlugin.RESULT_OK, plugin.readSingle("the-key"));
    }
}