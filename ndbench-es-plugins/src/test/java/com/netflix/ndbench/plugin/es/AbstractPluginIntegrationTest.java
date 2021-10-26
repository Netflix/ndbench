package com.netflix.ndbench.plugin.es;

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.ImmutableDockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import org.apache.commons.lang.StringUtils;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


/**
 * Enables integration tests to run such that Docker container initialization can be short-circuited in favor
 * of running a full Elasticsearch distribution locally, where such distribution is listening on standard ports.
 * <p>
 * To suppress start up of Elasticsearch in Docker, set the environment variable ES_NDBENCH_NO_DOCKER.
 * The main reason you would want to do this is that the current Docker configuration has some issues with
 * being run so that requests can be routed through an HTTP traffic proxy -- which is useful for debugging.
 */
public class AbstractPluginIntegrationTest extends AbstractPluginTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractPluginIntegrationTest.class);
    static final String ELASTICSEARCH = "elasticsearch";

    /**
     * Temporarily shut off mechanism to detect if docker and docker-compose are not available or not. If these
     * are not available, then we will disable running integration/smoke tests. Docker, and (even more likely)
     * docker-compose may be unavailable in some Jenkins and Travis CI environments.
     */
    protected static boolean isDockerAvailable = true;

    static {
        isCommandAvailable("docker");
        isCommandAvailable("docker-compose");
    }

    private static void isCommandAvailable(String command) {
        Process process = null;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            process.waitFor(5, TimeUnit.SECONDS);
            if (process.exitValue() != 0) {
                isDockerAvailable = false;
            }
        } catch (Exception e) {
            logger.error("Exception checking for command", e);
            isDockerAvailable = false;
        } finally {
            if (process != null) {
                process.destroyForcibly();
            }
        }
    }

    @ClassRule
    public static DockerComposeRule docker = getDockerComposeRule();

    private static ImmutableDockerComposeRule getDockerComposeRule() {
        if (!isDockerAvailable) {
            return null;
        }
        if (StringUtils.isNotEmpty(System.getenv("ES_NDBENCH_NO_DOCKER"))) {
            return null;
        }

        return DockerComposeRule.builder()
                .file("src/test/resources/docker-compose-elasticsearch.yml")
                .projectName(ProjectName.random())
                .waitingForService(ELASTICSEARCH, HealthChecks.toHaveAllPortsOpen())
                .build();
    }

    protected static DataGenerator alwaysSameValueGenerator = new DataGenerator() {
        @Override
        public String getRandomString() {
            return "hello";
        }

        @Override
        public String getRandomValue() {
            return "hello";
        }

        @Override
        public Integer getRandomInteger() {
            return 1;
        }

        @Override
        public Integer getRandomIntegerValue() {
            return 1;
        }
    };

    static EsRestPlugin getPlugin(String hostName,
                                  String indexName,
                                  boolean isBulkWrite,
                                  int indexRollsPerDay,
                                  int portNum) throws Exception {
        EsRestPlugin plugin = new EsRestPlugin(
                getCoreConfig(0, false, 60, 10, 10, 0.01f),
                getConfig(portNum, hostName, indexName, isBulkWrite, 0f, indexRollsPerDay),
                new MockServiceDiscoverer(9200),
                new GenericEsRestClient(),
                false);

        plugin.init(alwaysSameValueGenerator);

        return plugin;
    }
}

