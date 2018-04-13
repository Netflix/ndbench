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
import com.netflix.ndbench.api.plugin.NdBenchAbstractClient;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.NdBenchMonitor;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.DataBackfill;
import com.netflix.ndbench.core.NdBenchClientFactory;
import com.netflix.ndbench.core.NdBenchDriver;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.generators.KeyGenerator;
import com.netflix.ndbench.core.util.LoadPattern;
import com.sun.jersey.multipart.FormDataParam;
import groovy.lang.GroovyClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.netflix.ndbench.core.util.RestUtil.*;


/**
 * @author vchella, pencal
 */
@Path("/ndbench/driver")
public class NdBenchResource {

    private static final Logger logger = LoggerFactory.getLogger(NdBenchResource.class);

    private final NdBenchClientFactory clientFactory;
    private final NdBenchDriver ndBenchDriver;
    private final DataBackfill dataBackfill;
    private final IConfiguration config;
    private final NdBenchMonitor ndBenchMonitor;

    @Inject
    public NdBenchResource(NdBenchClientFactory cFactory, NdBenchDriver ndBenchDriver,
                           DataBackfill dataBackfill, IConfiguration config, NdBenchMonitor ndBenchMonitor) {
        this.clientFactory = cFactory;
        this.ndBenchDriver = ndBenchDriver;
        this.dataBackfill = dataBackfill;
        this.config = config;
        this.ndBenchMonitor  = ndBenchMonitor;
    }


    @Path("/initfromscript")
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response initfromscript(@FormDataParam("dynamicplugin") String dynamicPlugin) throws Exception {
        try {
            GroovyClassLoader gcl = new GroovyClassLoader();

            Class classFromScript = gcl.parseClass(dynamicPlugin);

            Object objectFromScript = classFromScript.newInstance();

            NdBenchClient client = (NdBenchClient) objectFromScript;

            ndBenchDriver.init(client);
            return sendSuccessResponse("NdBench client - dynamic plugin initiated with script!");

        } catch (Exception e) {
            logger.error("Error initializing dynamic plugin from script", e);
            return sendErrorResponse("script initialization failed for dynamic plugin!"+e);

        }
    }

    @Path("/startDataFill")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startDataFill() throws Exception {

        logger.info("Starting NdBench data fill");
        try {
            NdBenchAbstractClient<?> client = ndBenchDriver.getClient();
            dataBackfill.backfill(client);
            return sendSuccessResponse("data fill done!");
        } catch (Exception e) {
            logger.error("Error starting datafill", e);
            return  sendErrorResponse("dataFill failed!");
        }
    }

    @Path("/startDataFillAsync")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startDataFillAsync() throws Exception {

        logger.info("Starting NdBench data fill - Async");
        try {
            NdBenchAbstractClient<?> client = ndBenchDriver.getClient();
            dataBackfill.backfillAsync(client);
            return sendSuccessResponse( "Async data fill started !");
        } catch (Exception e) {
            logger.error("Error starting datafill", e);
            return  sendErrorResponse("Async dataFill failed to start!");
        }
    }

    @Path("/startConditionalDataFill")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response conditionalBackfill() throws Exception {

        logger.info("Starting NdBench data fill");
        try {
            NdBenchAbstractClient<?> client = ndBenchDriver.getClient();
            dataBackfill.conditionalBackfill(client);
            return sendSuccessResponse("data fill done!");
        } catch (Exception e) {
            logger.error("Error starting datafill", e);
            return sendErrorResponse("dataFill failed!");
        }
    }

    @Path("/startVerifyDataFill")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response verifyBackfill() throws Exception {

        logger.info("Starting NdBench data fill");
        try {
            NdBenchAbstractClient<?> client = ndBenchDriver.getClient();
            dataBackfill.verifyBackfill(client);
            return sendSuccessResponse("data fill done!");
        } catch (Exception e) {
            logger.error("Error starting datafill", e);
            return sendErrorResponse("dataFill failed!");
        }
    }

    @Path("/stopDataFill")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopDataFill() throws Exception {

        logger.info("stop NdBench data fill");
        try {
            dataBackfill.stopBackfill();
            return sendSuccessResponse("data fill stop!" );
        } catch (Exception e) {
            logger.error("Error stop datafill", e);
            return sendErrorResponse("dataFill failed!");
        }
    }



    @Path("/shutdownDataFill")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response shutdownDataFill() throws Exception {

        logger.info("shutdown NdBench data fill");
        try {
            dataBackfill.shutdown();
            return sendSuccessResponse("data fill stop!" );
        } catch (Exception e) {
            logger.error("Error shutdown datafill", e);
            return sendErrorResponse("dataFill failed!");
        }
    }

    @Path("/init/{client}")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response init(@PathParam("client") String clientName) throws Exception {
        try {
            NdBenchAbstractClient<?> client = clientFactory.getClient(clientName);
            ndBenchDriver.init(client);

            return sendSuccessResponse("NdBench client initiated!");
        } catch (Exception e) {
            logger.error("Error initializing the client - "+clientName, e);
            return sendErrorResponse("Client initialization failed!");
        }
    }

    @Path("/start")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response start(@DefaultValue("random") @QueryParam("loadPattern") String loadPattern,
                          @DefaultValue("-1")  @QueryParam("windowSize") int windowSize,
                          @DefaultValue("-1") @QueryParam("durationInSec") long durationInSec,
                          @DefaultValue("1") @QueryParam("bulkSize") int bulkSize) throws Exception {
        try {
            LoadPattern loadPatternType = LoadPattern.fromString(loadPattern);
            Result validationResult = validateLoadPatternParams(loadPatternType, windowSize, durationInSec);
            if (validationResult.isSuccess) {
                ndBenchDriver.start(loadPatternType, windowSize, durationInSec, bulkSize);
                logger.info("Starting NdBench test");
                return sendSuccessResponse("NDBench test started");
            } else {
                return sendResult(validationResult);
            }

        } catch (Exception e) {
            logger.error("Error starting NdBench test", e);
            return sendErrorResponse("NdBench start failed! " + e.getMessage());
        }
    }

    @Path("/startReads")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startReads(@DefaultValue("random") @QueryParam("loadPattern") String loadPattern,
                               @DefaultValue("-1")  @QueryParam("windowSize") int windowSize,
                               @DefaultValue("-1") @QueryParam("durationInSec") long durationInSec,
                               @DefaultValue("1") @QueryParam("bulkSize") int bulkSize) throws Exception {
        try {
            LoadPattern loadPatternType = LoadPattern.fromString(loadPattern);
            Result validationResult = validateLoadPatternParams(loadPatternType, windowSize, durationInSec);
            if (validationResult.isSuccess) {
                ndBenchDriver.startReads(loadPatternType, windowSize, durationInSec, bulkSize);
                logger.info("Starting NdBench reads");
                return sendSuccessResponse("NDBench reads started");
            } else {
                return sendResult(validationResult);
            }

        } catch (Exception e) {
            logger.error("Error starting NdBench read test", e);
            return sendErrorResponse("NdBench startReads failed! " + e.getMessage());
        }
    }
    @Path("/stopReads")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopReads() throws Exception {

        logger.info("stopping NdBenchread test");
        try {
            ndBenchDriver.stopReads();
            return sendSuccessResponse("NdBench reads stopped!");
        } catch (Exception e) {
            logger.error("Error stopping NdBench reads", e);
            return sendErrorResponse("NdBench stopreads failed! " + e.getMessage());
        }
    }

    @Path("/startWrites")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startWrites(@DefaultValue("random") @QueryParam("loadPattern") String loadPattern,
                                @DefaultValue("-1")  @QueryParam("windowSize") int windowSize,
                                @DefaultValue("-1") @QueryParam("durationInSec") long durationInSec,
                                @DefaultValue("1")  @QueryParam("bulkSize") int bulkSize) throws Exception {

        try {
            LoadPattern loadPatternType = LoadPattern.fromString(loadPattern);
            Result validationResult = validateLoadPatternParams(loadPatternType, windowSize, durationInSec);
            if (validationResult.isSuccess) {
                ndBenchDriver.startWrites(loadPatternType, windowSize, durationInSec, bulkSize);
                logger.info("Starting NdBench writes");
                return sendSuccessResponse("NDBench writes started");
            } else {
                return sendResult(validationResult);
            }

        } catch (Exception e) {
            logger.error("Error starting NdBench write test", e);
            return sendErrorResponse("NdBench startWrites failed! " + e.getMessage());
        }
    }
    @Path("/stopWrites")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopWrites() throws Exception {

        logger.info("stopping NdBenchwrite test");
        try {
            ndBenchDriver.stopWrites();
            return sendSuccessResponse("NdBench writes stopped!");
        } catch (Exception e) {
            logger.error("Error stopping NdBench writes", e);
            return sendErrorResponse("NdBench stopwrites failed! " + e.getMessage());
        }
    }

    @Path("/stop")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response stop() throws Exception {

        logger.info("Stopping NdBench tests");
        try {
            ndBenchDriver.stop();
            return sendSuccessResponse("NdBench test stopped!");
        } catch (Exception e) {
            logger.error("Error stopping NdBench test", e);
            return sendErrorResponse("NdBench stop failed! "+ e.getMessage());
        }
    }


    @Path("/readSingle/{key}")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response readSingle(@PathParam("key") String key) throws Exception {

        try {
            String value = ndBenchDriver.readSingle(key);

            return sendSuccessResponse(value);
        } catch (Exception e) {
            return sendErrorResponse("NdBench readSingle failed! " + e.getMessage());
        }
    }

    @Path("/writeSingle/{key}")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response writeSingle(@PathParam("key") String key) throws Exception {

        try {
            String result = ndBenchDriver.writeSingle(key);

            return sendSuccessResponse(result);
        } catch (Exception e) {
            logger.error("ERROR: " +  e.getMessage());
            return sendErrorResponse("NdBench writeSingle failed! " + e.getMessage());
        }
    }

    @Path("/stats")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response NdBenchStats() throws Exception {

        try {
            return sendJson(ndBenchMonitor);
        } catch (Exception e) {
            logger.error("Error getting NdBench stats", e);
            return sendErrorResponse("NdBench status failed! " + e.getMessage());
        }
    }

    @Path("/getReadStatus")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getReadStatus() throws Exception {

        try {
            if (ndBenchDriver.getIsReadRunning())
                return sendSuccessResponse("Read process running");
            else return sendSuccessResponse( "No Read process is running");
        } catch (Exception e) {
            logger.error("Error getting NdBench getReadStatus", e);
            return sendErrorResponse("NdBench getReadStatus failed! " + e.getMessage());
        }
    }

    @Path("/getWriteStatus")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWriteStatus() throws Exception {

        try {
            if (ndBenchDriver.getIsWriteRunning())
                return sendSuccessResponse("Writes process running");
            else  return sendSuccessResponse("No Write process is running");
        } catch (Exception e) {
            logger.error("Error getting NdBench getWriteStatus", e);
            return sendErrorResponse("NdBench getWriteStatus failed! " + e.getMessage());
        }
    }
    @Path("/shutdownclient")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response shutdownClient() throws Exception {

        try {
            ndBenchDriver.stop();
            ndBenchDriver.shutdownClient();
            ndBenchMonitor.resetStats();
            return sendSuccessResponse("NdBench client uninitialized");

        } catch (Exception e) {
            logger.error("Error shutting down NdBench client", e);
            return sendErrorResponse("NdBench shutdownClient failed! " + e.getMessage());
        }
    }

    @Path("/getdrivers")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDrivers() throws Exception {

        try {
            return sendJson(clientFactory.getClientDrivers());
        } catch (Exception e) {
            logger.error("Error in getting Client drivers", e);
            return sendErrorResponse("NdBench getDrivers failed! " + e.getMessage());
        }
    }
    @Path("/getserverstatus")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getServerStatus() throws Exception {

        try {
            Map<String, Object> serverStatusJson = new HashMap<>();

            serverStatusJson.put("ClientDrivers",clientFactory.getClientDrivers());
            serverStatusJson.put("LoadPatterns", Arrays.asList(LoadPattern.values()));
            String currentRunningDriver="NA",connectionInfo="NA", currentWriteLoadPattern="NA", currentReadLoadPattern="NA";

            NdBenchAbstractClient<?> NdBenchClient= ndBenchDriver.getClient();

            if(NdBenchClient!=null)
            {
                if(NdBenchClient.getClass().getAnnotation(NdBenchClientPlugin.class)!=null)
                {
                    currentRunningDriver=NdBenchClient.getClass().getAnnotation(NdBenchClientPlugin.class).value();
                }
                else
                {
                    currentRunningDriver=NdBenchClient.getClass().getSimpleName();
                }

                connectionInfo=NdBenchClient.getConnectionInfo();

            }
            KeyGenerator writeLoadPattern=ndBenchDriver.getWriteLoadPattern();
            if(null!=writeLoadPattern)
            {
                currentWriteLoadPattern= writeLoadPattern.getClass().getSimpleName();
            }

            KeyGenerator readLoadPattern=ndBenchDriver.getReadLoadPattern();
            if(null!=readLoadPattern)
            {
                currentReadLoadPattern= readLoadPattern.getClass().getSimpleName();
            }
            serverStatusJson.put("RunningDriver",currentRunningDriver);
            serverStatusJson.put("RunningWriteLoadPattern",currentWriteLoadPattern);
            serverStatusJson.put("RunningReadLoadPattern",currentReadLoadPattern);
            serverStatusJson.put("ConnectionInfo",connectionInfo);
            serverStatusJson.put("IsReadsRunning", ndBenchDriver.getIsReadRunning());
            serverStatusJson.put("IsWritesRunning", ndBenchDriver.getIsWriteRunning());
            serverStatusJson.put("Stats",ndBenchMonitor);
            serverStatusJson.put("DriverConfig",config);
            serverStatusJson.put("IsBackfillRunning",dataBackfill.getIsBackfillRunning());


            return sendJson(serverStatusJson);

        } catch (Exception e) {
            logger.error("Error in getting getServerStatus", e);
            return sendErrorResponse("NdBench getServerStatus failed! " + e.getMessage());
        }
    }


    @Path("/runworkflow")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response runWorkflow() throws Exception {

        try {
            NdBenchAbstractClient<?> client = ndBenchDriver.getClient();
            return sendSuccessResponse(client.runWorkFlow());
        } catch (Exception e) {
            logger.error("Error in running workflow", e);
            return sendErrorResponse("NdBench runworkflow failed! " + e.getMessage());
        }
    }

    private Result validateLoadPatternParams(LoadPattern loadPattern, long windowSize, long durationInSec)
    {
        String returnMsg = "Input validation Failure:";
        if(loadPattern==null)
        {
            returnMsg+="loadpattern parameter is not available";
            logger.error(returnMsg);
            return new ErrorResponse(returnMsg);
        }
        if(loadPattern.equals(LoadPattern.SLIDING_WINDOW)) {
            if (windowSize < 1 || durationInSec < 1) {
                returnMsg+="WindowSize and DurationInSeconds can not be less than 1, provided: windowSize: "+windowSize+", durationInSec: "+durationInSec;
                logger.error(returnMsg);
                return new ErrorResponse(returnMsg);
            }
        }
        return new SuccessResponse("");
    }
}