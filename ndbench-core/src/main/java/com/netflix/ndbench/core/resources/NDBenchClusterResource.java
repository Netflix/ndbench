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
import com.netflix.ndbench.core.discovery.IClusterDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.netflix.ndbench.core.util.RestUtil.sendErrorResponse;
import static com.netflix.ndbench.core.util.RestUtil.sendJson;

/**
 * @author vchella
 */
@Path("/ndbench/cluster")
public class NDBenchClusterResource {
    private static final Logger logger = LoggerFactory.getLogger(NDBenchClusterResource.class);

    private final IClusterDiscovery clusterManager;

    @Context
    HttpServletRequest request;


    @Inject
    public NDBenchClusterResource(IClusterDiscovery clusterManager) {
        this.clusterManager = clusterManager;

    }
    @Path("/list")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getApps() throws Exception {

        logger.info("Getting cluster list");
        try {
            return sendJson(clusterManager.getApps());
        } catch (Exception e) {
            logger.error("Error getting Apps list from ClusterManager", e);
            return  sendErrorResponse("get cluster/list failed!");
        }
    }
    @Path("/{appname}/list")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getApps(@PathParam("appname") String appname) throws Exception {

        logger.info("Getting nodes list for app: "+appname+", default Port used: "+ request.getServerPort());
        try {
            return sendJson(clusterManager.getEndpoints(appname, request.getServerPort()));
        } catch (Exception e) {
            logger.error("Error getting Host list from ClusterManager for app: "+appname, e);
            return  sendErrorResponse("get cluster host list failed!");
        }
    }

}
