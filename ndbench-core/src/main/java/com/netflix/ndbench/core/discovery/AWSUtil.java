/**
 * Copyright (c) 2018 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.core.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author vchella
 */
public class AWSUtil {
    private static final Logger logger = LoggerFactory.getLogger(LocalClusterDiscovery.class.getName());

    public static String getLocalhostName() {
        String urlPublic = "http://169.254.169.254/latest/meta-data/public-hostname";
        String urlLocal = "http://169.254.169.254/latest/meta-data/local-hostname";
        try {
            return parseAwsMetadataByURL(urlPublic);
        }
        catch (Exception e) {
            logger.error("Unable to get the public hostname name. Trying local...",e);
            return parseAwsMetadataByURL(urlLocal);
        }
    }

    public static String getLocalInstanceId() {
        String instanceId = "http://169.254.169.254/latest/meta-data/instance-id";

        try {
            return parseAwsMetadataByURL(instanceId);
        }
        catch (Exception e) {
            logger.error("Unable to get the public hostname name. Trying local...",e);
        }
        return null;
    }


    private static String parseAwsMetadataByURL(String urlPublic){
        BufferedReader in = null;
        try{
            HttpURLConnection con = (HttpURLConnection) new URL(urlPublic).openConnection();
            con.setRequestMethod("GET");

            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            return response.toString().trim();
        }catch(Exception e){
            throw new RuntimeException(e);
        }finally{
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
