package com.netflix.ndbench.core.discovery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class does Cluster discovery at AWS VPC cloud env. <BR>
 * First try to resolve the public-hostname if present otherwise it gets the local-hostname IP address. 
 * 
 * @author diegopacheco
 * @since 10/20/2016
 *
 */
public class AWSLocalClusterDiscovery implements IClusterDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(LocalClusterDiscovery.class.getName());

    @Override
    public List<String> getApps() {
        return Arrays.asList(getLocalhostName());
    }

    private String getLocalhostName() {
    	String urlPublic = "http://169.254.169.254/latest/meta-data/public-hostname";
    	String urlLocal = "http://169.254.169.254/latest/meta10-data/local-hostname";
        try {
    		return parseAwsMetadataByURL(urlPublic);
        }
        catch (Exception e) {
            logger.error("Unable to get the public hostname name. Trying local...",e);
            return parseAwsMetadataByURL(urlLocal);
        }
    }

	private String parseAwsMetadataByURL(String urlPublic){
		BufferedReader in = null;
		try{
			HttpURLConnection con = (HttpURLConnection) new URL(urlPublic).openConnection();
			con.setRequestMethod("GET");

			in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
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
    
    @Override
    public List<String> getEndpoints(String appName) {
        return Arrays.asList(getLocalhostName()+":8080");
    }

}