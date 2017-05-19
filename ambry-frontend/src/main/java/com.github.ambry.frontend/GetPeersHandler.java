/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.SystemTime;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler for requests that require the peer datanodes of a given datanode.
 */
class GetPeersHandler {
  static final String NAME_QUERY_PARAM = "name";
  static final String PORT_QUERY_PARAM = "port";
  private static final Logger LOGGER = LoggerFactory.getLogger(GetPeersHandler.class);

  private final ClusterMap clusterMap;
  private final SecurityService securityService;
  private final FrontendMetrics metrics;

  /**
   * Constructs a handler for handling requests for peers.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param securityService the {@link SecurityService} to use.
   * @param metrics {@link FrontendMetrics} instance where metrics should be recorded.
   */
  GetPeersHandler(ClusterMap clusterMap, SecurityService securityService, FrontendMetrics metrics) {
    this.clusterMap = clusterMap;
    this.securityService = securityService;
    this.metrics = metrics;
  }

  /**
   * Handles a request for peers of a given datanode. Expects the arguments to have {@link #NAME_QUERY_PARAM} and
   * {@link #PORT_QUERY_PARAM}. Returns the peers as comma separated list of host:port pairs in the response body.
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response {@link ReadableStreamChannel} is ready (or if
   *                 there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    RestRequestMetrics requestMetrics =
        restRequest.getSSLSession() != null ? metrics.getPeersMetrics : metrics.getPeersSSLMetrics;
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    securityService.processRequest(restRequest,
        new SecurityProcessRequestCallback(restRequest, restResponseChannel, callback));
  }

  /**
   * Callback for the {@link SecurityService} that handles generating the replica list if the security checks
   * succeeded.
   */
  private class SecurityProcessRequestCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> callback;
    private final long operationStartTimeMs;

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
      operationStartTimeMs = SystemTime.getInstance().milliseconds();
    }

    /**
     * If {@code exception} is null, gathers all the peers of the datanode that corresponds to the params in the request
     * and returns them as a part of the response body.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.getPeersSecurityProcessingTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          DataNodeId dataNodeId = getDataNodeId(restRequest);
          LOGGER.trace("Getting peer hosts for {}:{}", dataNodeId.getHostname(), dataNodeId.getPort());
          Set<DataNodeId> peerDataNodeIds = new HashSet<>();
          List<? extends ReplicaId> replicaIdList = clusterMap.getReplicaIds(dataNodeId);
          for (ReplicaId replicaId : replicaIdList) {
            List<? extends ReplicaId> peerReplicaIds = replicaId.getPeerReplicaIds();
            for (ReplicaId peerReplicaId : peerReplicaIds) {
              peerDataNodeIds.add(peerReplicaId.getDataNodeId());
            }
          }
          StringBuilder peerSb = new StringBuilder();
          for (DataNodeId peerDataNodeId : peerDataNodeIds) {
            peerSb.append(peerDataNodeId.getHostname()).append(":").append(peerDataNodeId.getPort()).append(",");
          }
          if (peerSb.length() > 0) {
            peerSb.deleteCharAt(peerSb.length() - 1);
          }
          String peerStr = peerSb.toString();
          ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(peerStr.getBytes()));
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "text/plain");
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
          callback.onCompletion(channel, null);
        } else {
          callback.onCompletion(null, exception);
        }
      } catch (Exception e) {
        callback.onCompletion(null, e);
      } finally {
        metrics.getPeersProcessingTimeInMs.update(SystemTime.getInstance().milliseconds() - processingStartTimeMs);
      }
    }

    /**
     * Gets the {@link DataNodeId} based on query parameters in the {@code restRequest}. Return is always non-null.
     * @param restRequest the {@link RestRequest} containing the parameters of the request.
     * @return the {@link DataNodeId} based on query parameters in the {@code restRequest}. Return is always non-null.
     * @throws RestServiceException if either {@link #NAME_QUERY_PARAM} or {@link #PORT_QUERY_PARAM} is missing, if
     * {@link #PORT_QUERY_PARAM} is not an {@link Integer} or if there is no datanode that corresponds to the given
     * parameters.
     */
    private DataNodeId getDataNodeId(RestRequest restRequest) throws RestServiceException {
      String name = (String) restRequest.getArgs().get(NAME_QUERY_PARAM);
      String portStr = (String) restRequest.getArgs().get(PORT_QUERY_PARAM);
      if (name == null || portStr == null) {
        throw new RestServiceException("Missing name and/or port of data node", RestServiceErrorCode.MissingArgs);
      }
      int port;
      try {
        port = Integer.parseInt(portStr);
      } catch (NumberFormatException e) {
        throw new RestServiceException("Port " + "[" + portStr + "] could not parsed into a number",
            RestServiceErrorCode.InvalidArgs);
      }
      DataNodeId dataNodeId = clusterMap.getDataNodeId(name, port);
      if (dataNodeId == null) {
        metrics.unknownDatanodeError.inc();
        throw new RestServiceException("No datanode found for parameters " + name + ":" + port,
            RestServiceErrorCode.NotFound);
      }
      return dataNodeId;
    }
  }
}
