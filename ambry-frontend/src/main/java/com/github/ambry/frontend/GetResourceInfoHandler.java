/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.frontend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.ResourceInfo;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


public class GetResourceInfoHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GetResourceInfoHandler.class);
  private final SecurityService securityService;
  private final FrontendMetrics frontendMetrics;
  private final ClusterMap clustermap;
  private final boolean isHelixClusterMap;

  public static final String RESOURCES_KEY = "resources";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Constructs a handle for handling requests for getting resource info.
   * @param securityService The {@link SecurityService} to use.
   * @param frontendMetrics The {@link FrontendMetrics} to use.
   * @param clustermap The {@link ClusterMap} to use.
   */
  public GetResourceInfoHandler(SecurityService securityService, FrontendMetrics frontendMetrics,
      ClusterMap clustermap) {
    this.securityService = securityService;
    this.frontendMetrics = frontendMetrics;
    this.clustermap = clustermap;
    this.isHelixClusterMap = clustermap instanceof HelixClusterManager;
  }

  /**
   * Asynchronously get the resource info.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;
    private final String uri;

    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#preProcessRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          frontendMetrics.getResourceInfoMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      // Start the callback chain by performing request security processing.
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.getResourceInfoSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call the final callback with the response body to
     * send the serialized resource info back to client.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.getResourceInfoSecurityPostProcessRequestMetrics, securityCheckResult -> {
        if (!isHelixClusterMap) {
          finalCallback.onCompletion(null,
              new RestServiceException("Method is only supported when the cluster map is a helix based cluster map",
                  RestServiceErrorCode.NotAllowed));
        } else {
          Map<String, List<ResourceInfo>> resultObj = new HashMap<>();
          resultObj.put(RESOURCES_KEY, queryResourceInfo());
          byte[] serialized = objectMapper.writeValueAsBytes(resultObj);
          ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(serialized));
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
          finalCallback.onCompletion(channel, null);
        }
      }, uri, LOGGER, finalCallback);
    }

    private List<ResourceInfo> queryResourceInfo() throws RestServiceException {
      String partitionName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.PARTITION, false);
      String hostname = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.HOSTNAME, false);
      String resourceName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.RESOURCE, false);
      HelixClusterManager.ResourceIdentifier resourceIdentifier;
      if (partitionName != null) {
        resourceIdentifier = new HelixClusterManager.PartitionIdIdentifier(partitionName);
      } else if (hostname != null) {
        resourceIdentifier = new HelixClusterManager.HostnameIdentifier(hostname);
      } else if (resourceName != null) {
        resourceIdentifier = new HelixClusterManager.ResourceNameIdentifier(resourceName);
      } else {
        resourceIdentifier = new HelixClusterManager.AllResourceIdentifier();
      }
      try {
        return ((HelixClusterManager) clustermap).queryResourceInfos(resourceIdentifier);
      } catch (IllegalArgumentException e) {
        throw new RestServiceException(e.getMessage(), e, RestServiceErrorCode.BadRequest);
      } catch (Exception e) {
        throw new RestServiceException(e.getMessage(), e, RestServiceErrorCode.InternalServerError);
      }
    }
  }
}
