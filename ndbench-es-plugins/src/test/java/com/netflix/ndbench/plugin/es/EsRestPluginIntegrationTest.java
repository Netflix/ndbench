package com.netflix.ndbench.plugin.es;

import com.netflix.governator.guice.test.ModulesForTesting;
import com.netflix.governator.guice.test.junit4.GovernatorJunit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GovernatorJunit4ClassRunner.class)
@ModulesForTesting({})
public class EsRestPluginIntegrationTest extends AbstractPluginIntegrationTest {

    static final String ELASTICSEARCH = "elasticsearch";


    private static final Logger logger = LoggerFactory.getLogger(EsRestPluginIntegrationTest.class);
    private static final String ES_HOST_PORT = "http://localhost:9200";


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

