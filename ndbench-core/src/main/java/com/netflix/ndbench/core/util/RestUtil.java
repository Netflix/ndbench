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

import javax.ws.rs.core.Response;

/**
 * @author vchella
 */
public class RestUtil {


    public static Response sendResult(Response.Status status, Result result) {
        return sendResponse(status, result);
    }
    public static Response sendResult(int status, Result result) {
        return sendResponse(status, result);
    }
    public static Response sendResult(Result result) {
        return sendResult(result.isSuccess?Response.Status.OK:Response.Status.INTERNAL_SERVER_ERROR,result);
    }
    public static Response sendErrorResponse(String errorMessage)
    {
        return sendResponse(Response.Status.INTERNAL_SERVER_ERROR, new ErrorResponse(errorMessage));
    }
    public static Response sendSuccessResponse(String returnMessage)
    {
        return sendResponse(Response.Status.OK, new SuccessResponse(returnMessage));
    }
    public static Response sendErrorResponse()
    {
        return sendResponse(Response.Status.INTERNAL_SERVER_ERROR, new ErrorResponse("Unknown error occurred."));
    }
     static <T> Response sendResponse(Response.Status status, T object)
    {
        return Response.status(status).type(javax.ws.rs.core.MediaType.APPLICATION_JSON).entity(object)
                .header("Access-Control-Allow-Origin","*")
                .header("Access-Control-Allow-Headers","Content-Type, content-type")
                .header("Access-Control-Allow-Method","OPTIONS, GET, POST")
                .build();
    }
     static <T> Response sendResponse(int status, T object)
    {
        return Response.status(status).type(javax.ws.rs.core.MediaType.APPLICATION_JSON).entity(object)
                .header("Access-Control-Allow-Origin","*")
                .header("Access-Control-Allow-Headers","Content-Type, content-type")
                .header("Access-Control-Allow-Method","OPTIONS, GET, POST")
                .build();
    }

    public static <T> Response sendJson(T object)
    {
        return sendResponse(Response.Status.OK, object);
    }
    public static class ErrorResponse extends Result
    {
        public ErrorResponse(String errorMessage)
        {

            super(false,errorMessage);
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
        String message;

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
