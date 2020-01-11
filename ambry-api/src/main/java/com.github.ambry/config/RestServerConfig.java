/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.config;

/**
 * Configuration parameters required by RestServer and Rest infrastructure
 */
public class RestServerConfig {
  /**
   * The RestRequestServiceFactory that needs to be used by the RestServer
   * for bootstrapping the RestRequestService.
   */
  @Config("rest.server.rest.request.service.factory")
  public final String restServerRestRequestServiceFactory;

  /**
   * The NioServerFactory that needs to be used by the RestServer or AmbryServer for
   * bootstrapping the Frontend NioServer or Storage Node HTTP2 Server.
   */
  @Config("rest.server.nio.server.factory")
  @Default("com.github.ambry.rest.FrontendNettyFactory")
  public final String restServerNioServerFactory;

  /**
   * The number of scaling units in RestRequestHandler that will handle requests.
   */
  @Config("rest.server.request.handler.scaling.unit.count")
  @Default("5")
  public final int restServerRequestHandlerScalingUnitCount;

  /**
   * The RequestResponseHandlerFactory that needs to be used by the RestServer or AmbryServer HTTP2
   * for bootstrapping the RestRequestHandler and RestResponseHandler
   */
  @Config("rest.server.request.response.handler.factory")
  @Default("com.github.ambry.rest.AsyncRequestResponseHandlerFactory")
  public final String restServerRequestResponseHandlerFactory;

  /**
   * The number of scaling units in RestResponseHandler handle responses.
   */
  @Config("rest.server.response.handler.scaling.unit.count")
  @Default("5")
  public final int restServerResponseHandlerScalingUnitCount;

  /**
   * The AccountServiceFactory that needs to be used by FrontendRestRequestService to get account-related information.
   */
  @Config("rest.server.account.service.factory")
  @Default("com.github.ambry.account.InMemoryUnknownAccountServiceFactory")
  public final String restServerAccountServiceFactory;

  /**
   * The RouterFactory that needs to be used by the RestServer
   * for bootstrapping the Router.
   */
  @Config("rest.server.router.factory")
  @Default("com.github.ambry.router.NonBlockingRouterFactory")
  public final String restServerRouterFactory;

  /**
   * Request Headers that needs to be logged as part of public access log entries
   */
  @Config("rest.server.public.access.log.request.headers")
  @Default("Host,Referer,User-Agent,Content-Length,x-ambry-content-type,x-ambry-owner-id,x-ambry-ttl,x-ambry-private,x-ambry-service-id,X-Forwarded-For")
  public final String restServerPublicAccessLogRequestHeaders;

  /**
   * Response Headers that needs to be logged as part of public access log entries
   */
  @Config("rest.server.public.access.log.response.headers")
  @Default("Location,x-ambry-blob-size")
  public final String restServerPublicAccessLogResponseHeaders;

  /**
   * Health check URI for load balancers (VIPs)
   */
  @Config("rest.server.health.check.uri")
  @Default("/healthCheck")
  public final String restServerHealthCheckUri;

  public RestServerConfig(VerifiableProperties verifiableProperties) {
    restServerRestRequestServiceFactory = verifiableProperties.getString("rest.server.rest.request.service.factory");
    restServerNioServerFactory =
        verifiableProperties.getString("rest.server.nio.server.factory", "com.github.ambry.rest.FrontendNettyFactory");
    restServerRequestHandlerScalingUnitCount =
        verifiableProperties.getIntInRange("rest.server.request.handler.scaling.unit.count", 5, 0, Integer.MAX_VALUE);
    restServerRequestResponseHandlerFactory = verifiableProperties.getString("rest.server.request.response.handler.factory",
        "com.github.ambry.rest.AsyncRequestResponseHandlerFactory");
    restServerResponseHandlerScalingUnitCount =
        verifiableProperties.getIntInRange("rest.server.response.handler.scaling.unit.count", 5, 0, Integer.MAX_VALUE);
    restServerAccountServiceFactory = verifiableProperties.getString("rest.server.account.service.factory",
        "com.github.ambry.account.InMemoryUnknownAccountServiceFactory");
    restServerRouterFactory = verifiableProperties.getString("rest.server.router.factory",
        "com.github.ambry.router.NonBlockingRouterFactory");
    restServerPublicAccessLogRequestHeaders =
        verifiableProperties.getString("rest.server.public.access.log.request.headers",
            "Host,Referer,User-Agent,Content-Length,x-ambry-content-type,x-ambry-owner-id,x-ambry-ttl,x-ambry-private,x-ambry-service-id,X-Forwarded-For");
    restServerPublicAccessLogResponseHeaders =
        verifiableProperties.getString("rest.server.public.access.log.response.headers", "Location,x-ambry-blob-size");
    restServerHealthCheckUri = verifiableProperties.getString("rest.server.health.check.uri", "/healthCheck");
  }
}
