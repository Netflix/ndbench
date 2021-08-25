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
package com.netflix.ndbench.core.util;

import com.netflix.ndbench.core.config.IConfiguration;
import org.apache.log4j.DefaultThrowableRenderer;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author vchella
 */
public class RestUtil {
    public static Response sendResult(Response.Status status, Result result, IConfiguration config) {
        return sendResponse(status, result, config);
    }
    public static Response sendResult(Result result, IConfiguration config) {
        return sendResult(result.isSuccess?Response.Status.OK:Response.Status.INTERNAL_SERVER_ERROR,result, config);
    }
    public static Response sendErrorResponse(String errorMessage, Exception exception, IConfiguration config)
    {
        return sendResponse(Response.Status.INTERNAL_SERVER_ERROR, new ErrorResponse(errorMessage, exception), config);
    }
    public static Response sendErrorResponse(String errorMessage, IConfiguration config)
    {
        return sendResponse(Response.Status.INTERNAL_SERVER_ERROR, new ErrorResponse(errorMessage), config);
    }
    public static Response sendSuccessResponse(String returnMessage, IConfiguration config)
    {
        return sendResponse(Response.Status.OK, new SuccessResponse(returnMessage), config);
    }
    public static Response sendErrorResponse(IConfiguration config)
    {
        return sendResponse(Response.Status.INTERNAL_SERVER_ERROR, new ErrorResponse("Unknown error occurred."), config);
    }
    static <T> Response sendResponse(Response.Status status, T object, IConfiguration config)
    {
        Response.ResponseBuilder builder = Response.status(status).type(javax.ws.rs.core.MediaType.APPLICATION_JSON).entity(object);
        return builder.build();
    }

    public static <T> Response sendJson(T object, IConfiguration config)
    {
        return sendResponse(Response.Status.OK, object, config);
    }
    public static class ErrorResponse extends Result
    {
        public String detailedMessage = "NA";

        public ErrorResponse(String errorMessage)
        {

            super(false,errorMessage);
        }

        public ErrorResponse(String errorMessage, Exception e)
        {
            super(false, errorMessage);
            makeMessage(e);
        }

        private void makeMessage(Exception e) {
            if (e != null) {
                this.message = this.message + " " + e.getMessage() + " !!!  ";
                if (e.getCause() != null) {
                    this.message += e.getCause().getMessage();
                }
                DefaultThrowableRenderer dtr = new DefaultThrowableRenderer();
                detailedMessage = Arrays.stream(dtr.doRender(e)).collect(Collectors.joining("\n"));
            }
        }
    }
    public static class SuccessResponse extends Result
    {
        public SuccessResponse(String successMessage)
        {
            super(true,successMessage);
        }
    }

   public static class Result
    {
        public boolean isSuccess;
        public String message;

        Result(boolean result, String resultMessage)
        {
            this.isSuccess = result;
            this.message=resultMessage;
        }

        public Result(boolean result)
        {
            this.isSuccess = result;
            this.message="NA";
        }
    }

}
