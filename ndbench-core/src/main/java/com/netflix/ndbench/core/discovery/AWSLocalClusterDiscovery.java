/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.ndbench.core.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

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
    public List<String> getEndpoints(String appName, int defaultPort) {
        return Arrays.asList(getLocalhostName()+":"+defaultPort);
    }

}