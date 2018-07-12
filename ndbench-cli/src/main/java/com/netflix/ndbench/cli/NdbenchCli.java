/*
 *  Copyright 2018 Netflix
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.ndbench.cli;

import com.google.inject.Injector;
import com.netflix.ndbench.cli.config.CliConfigs;
import com.netflix.ndbench.core.NdBenchClientFactory;
import com.netflix.ndbench.core.NdBenchDriver;
import com.netflix.ndbench.core.config.GuiceInjectorProvider;
import com.netflix.ndbench.core.util.LoadPattern;
import org.slf4j.LoggerFactory;

/**
 * This class is a CLI entry point to facilitate quick testing of the Netflix Data Benchmark (NdBench).
 * In particular, this class does not require deploying a WAR to Tomcat to run the benchmark.
 * Nor does this class require running a Context in a web container.
 *
 * @author Alexander Patrikalakis
 */
public class NdbenchCli {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NdbenchCli.class);
    public static void main(final String[] argv) {
        Injector injector = new GuiceInjectorProvider().getInjector(new CliModule());
        CliConfigs cliConfigs = injector.getInstance(CliConfigs.class);
        NdBenchDriver driver = injector.getInstance(NdBenchDriver.class);

        try {
            driver.init(injector.getInstance(NdBenchClientFactory.class).getClient(cliConfigs.getClientName()));
            long millisToWait = Integer.valueOf(cliConfigs.getCliTimeoutMillis());

            logger.info("Starting driver in CLI with loadPattern=" + cliConfigs.getLoadPattern()
                    + ", windowSize=" + cliConfigs.getWindowSize()
                    + ", windowDurationInSec=" + cliConfigs.getWindowDurationInSec()
                    + ", bulkSize=" + cliConfigs.getBulkSize()
                    + ", timeout(ms)=" + (millisToWait == 0L ? "no timeout" : cliConfigs.getCliTimeoutMillis())

                    + ", clientName=" + cliConfigs.getClientName());
            driver.start(
                    LoadPattern.fromString(cliConfigs.getLoadPattern()),
                    Integer.valueOf(cliConfigs.getWindowSize()),
                    Integer.valueOf(cliConfigs.getWindowDurationInSec()),
                    Integer.valueOf(cliConfigs.getBulkSize())
            );

            if (millisToWait > 0) {
                logger.info("Waiting " + millisToWait + " ms for reads and writes to finish");
                Thread.sleep(millisToWait); //blocking
                logger.info("Waited " + millisToWait + " ms for reads and writes to finish. Stopping driver.");
                driver.stop(); //blocking
                logger.info("Stopped driver");
                System.exit(0);
            }
        } catch(Exception e) {
            logger.error("Encountered an exception when driving load", e);
            System.exit(-1);
        }
    }
}
