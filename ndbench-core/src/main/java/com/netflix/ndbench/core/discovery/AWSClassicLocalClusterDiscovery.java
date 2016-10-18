package com.netflix.ndbench.core.discovery;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSClassicLocalClusterDiscovery implements IClusterDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(LocalClusterDiscovery.class.getName());

    @Override
    public List<String> getApps() {
        return Arrays.asList(getLocalhostName());
    }

    private String getLocalhostName() {
    	String url = "http://169.254.169.254/latest/meta-data/public-hostname";
        try {
    		HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
    		con.setRequestMethod("GET");

    		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    		String inputLine;
    		StringBuffer response = new StringBuffer();
    		while ((inputLine = in.readLine()) != null) {
    			response.append(inputLine);
    		}
    		in.close();
    		return response.toString().trim();
        }
        catch (Exception e) {
            logger.error("Unable to get the localhost name. ",e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public List<String> getEndpoints(String appName) {
        return Arrays.asList(getLocalhostName()+":8080");
    }

}