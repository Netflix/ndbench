package com.netflix.ndbench.core.filters;

import com.netflix.ndbench.core.config.IConfiguration;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.ext.Provider;
import java.util.Arrays;
import java.util.List;

@Provider
public class CorsResponseFilter implements ContainerResponseFilter {
  public static final Logger LOGGER = LoggerFactory.getLogger(CorsResponseFilter.class);
  @Inject IConfiguration config;

  @Override
  public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
    List<String> allowedOrigins = Arrays.asList(config.getAllowedOrigins().split(";"));
    String origin = request.getRequestHeaders().getFirst("Origin");
    if (allowedOrigins.contains(origin)) {
      response.getHttpHeaders().add("Access-Control-Allow-Origin", origin);
      response.getHttpHeaders().add("Vary", "Origin");
    }
    return response;
  }
}
