package com.test;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class Log4jConfigurationTest {
    static {
        try {
            File temp = File.createTempFile("temp-file-name", ".log");
            temp.deleteOnExit();
            FileUtils.writeStringToFile(temp, "log4j.logger.com.test=TRACE");
            System.setProperty(
                    "log4j.configuration",
                    "file:///" + temp.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void verifyLog4jPropertiesConfigurationWorksAsExpected() throws Exception {
        final Logger logger = LoggerFactory.getLogger(Log4jConfigurationTest .class);
        if (! logger.isTraceEnabled()) {
            throw new RuntimeException("slf4j seems not to be bound to a log4j implementation. check depdendencies!");
        }
    }
}

