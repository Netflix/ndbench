/*
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.ndbench.core.resources;

import com.google.inject.Inject;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.api.inject.RuntimeLayer;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;
import com.netflix.ndbench.core.NdBenchDriver;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.config.TunableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

import static com.netflix.ndbench.core.util.RestUtil.*;


/**
 * @author vchella
 */
@Path("/ndbench/config")
public class NDBenchConfigResource {
    private static final Logger logger = LoggerFactory.getLogger(NDBenchConfigResource.class);

    private final IConfiguration config;
    private final NdBenchDriver ndBenchDriver;
    private final SettableConfig settableConfig;

    @Inject
    public NDBenchConfigResource(IConfiguration config,
                                 NdBenchDriver ndBenchDriver,
                                 @RuntimeLayer SettableConfig settableConfig
                                 ) {
        this.config = config;
        this.ndBenchDriver = ndBenchDriver;
        this.settableConfig = settableConfig;

    }
    @Path("/list")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfigOptions() throws Exception {

        logger.info("Getting Configuration list");
        try {
            return sendJson(config);
        } catch (Exception e) {
            logger.error("Error getting Configuration", e);
            return  sendErrorResponse("get config/list failed!");
        }
    }

    @Path("/set")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response setConfigOptions(Map<String,String> propsToSet) throws Exception {

        logger.info("Setting Configuration list");
        try {
            for (Map.Entry<String,String> entry: propsToSet.entrySet())
            {
                if (entry.getKey()!=null && !entry.getKey().isEmpty()
                        && entry.getValue()!=null && !entry.getValue().isEmpty()) {
                    settableConfig.setProperty(NdBenchConstants.PROP_NAMESPACE + entry.getKey(), entry.getValue());

                }
            }
            return sendSuccessResponse("Properties have been applied");
        } catch (Exception e) {
            logger.error("Error setting Configuration", e);
            return  sendErrorResponse("get config/set failed!");
        }
    }

    @Path("/set")
    @OPTIONS
    public Response setConfigOptionsPreflight() throws Exception {
        return sendSuccessResponse("OK");
    }

    @Path("/tunable/list")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTunableOptions() throws Exception {

        logger.info("Getting Tunable Configuration list");
        try {

            TunableConfig tunableConfig = new TunableConfig(config);
            return sendJson(tunableConfig);
        } catch (Exception e) {
            logger.error("Error getting Configuration", e);
            return  sendErrorResponse("get config/list failed!");
        }
    }

    @Path("/tunable/set")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response setTunableOptions(Map<String,String> propsToSet) throws Exception {

        logger.info("Setting Tunable Configuration list");
        try {
            for (Map.Entry<String,String> entry: propsToSet.entrySet())
            {
                if (entry.getKey()!=null && !entry.getKey().isEmpty()
                        && entry.getValue()!=null && !entry.getValue().isEmpty()) {

                        settableConfig.setProperty(NdBenchConstants.PROP_NAMESPACE +entry.getKey(), entry.getValue());
                    switch (entry.getKey())
                    {
                        case NdBenchConstants.READ_RATE_LIMIT:
                            ndBenchDriver.onReadRateLimitChange();
                            break;
                        case NdBenchConstants.WRITE_RATE_LIMIT:
                            ndBenchDriver.onWriteRateLimitChange();
                            break;
                    }
                }
            }
            return sendSuccessResponse("Tunable Properties have been applied");
        } catch (Exception e) {
            logger.error("Error setting Tunable Configuration", e);
            return  sendErrorResponse("get config/tunable/set failed!");
        }
    }

    @Path("/tunable/set")
    @OPTIONS
    public Response setTunableOptionsPreflight() throws Exception {
        return sendSuccessResponse("OK");
    }

}
