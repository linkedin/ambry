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
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.SystemTime;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler for requests that require the peer datanodes of a given datanode.
 */
class GetPeersHandler {
  static final String NAME_QUERY_PARAM = "name";
  static final String PORT_QUERY_PARAM = "port";
  static final String PEERS_FIELD_NAME = "peers";
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
   * {@link #PORT_QUERY_PARAM}. Returns the peers as a JSON with the field {@link #PEERS_FIELD_NAME} whose value is a
   * JSON array of objects with fields {@link #NAME_QUERY_PARAM} and {@link #PORT_QUERY_PARAM}.
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response {@link ReadableStreamChannel} is ready (or if
   *                 there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    RestRequestMetrics requestMetrics =
        metrics.getPeersMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    securityService.processRequest(restRequest,
        new SecurityProcessRequestCallback(restRequest, restResponseChannel, callback));
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

  /**
   * Constructs the response body as a JSON that contains the details of all the {@code dataNodeIds}.
   * @param dataNodeIds the {@link DataNodeId}s that need to be packaged as peers.
   * @return a {@link ReadableStreamChannel} containing the response.
   * @throws JSONException if there is any problem with JSON construction or manipulation.
   */
  private static ReadableStreamChannel getResponseBody(Set<DataNodeId> dataNodeIds)
      throws JSONException, RestServiceException {
    JSONObject peers = new JSONObject();
    peers.put(PEERS_FIELD_NAME, new JSONArray());
    for (DataNodeId dataNodeId : dataNodeIds) {
      JSONObject peer = new JSONObject();
      peer.put(NAME_QUERY_PARAM, dataNodeId.getHostname());
      peer.put(PORT_QUERY_PARAM, dataNodeId.getPort());
      peers.append(PEERS_FIELD_NAME, peer);
    }
    return FrontendUtils.serializeJsonToChannel(peers);
  }

  /**
   * Callback for {@link SecurityService#processRequest(RestRequest, Callback)} that subsequently calls
   * {@link SecurityService#postProcessRequest(RestRequest, Callback)}. If post processing succeeds, the replica
   * list will be generated.
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
     * If {@code exception} is null, call {@link SecurityService#postProcessRequest(RestRequest, Callback)}.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.getPeersSecurityRequestTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          securityService.postProcessRequest(restRequest,
              new SecurityPostProcessRequestCallback(restRequest, restResponseChannel, callback));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.getPeersSecurityRequestCallbackProcessingTimeInMs.update(
            SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        if (exception != null) {
          callback.onCompletion(null, exception);
        }
      }
    }
  }

  /**
   * Callback for {@link SecurityService#postProcessRequest(RestRequest, Callback)} that handles generating the replica
   * list if the security checks succeeded.
   */
  private class SecurityPostProcessRequestCallback implements Callback<Void> {

    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> callback;
    private final long operationStartTimeMs;

    SecurityPostProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
      operationStartTimeMs = SystemTime.getInstance().milliseconds();
    }

    /**
     * If {@code exception} is null, gathers all the peers of the datanode that corresponds to the params in the request
     * and returns them as a JSON with the field {@link #PEERS_FIELD_NAME} whose value is a JSON array of objects with
     * fields {@link #NAME_QUERY_PARAM} and {@link #PORT_QUERY_PARAM}.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.getPeersSecurityPostProcessRequestTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      ReadableStreamChannel channel = null;
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
          channel = getResponseBody(peerDataNodeIds);
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.getPeersProcessingTimeInMs.update(SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        callback.onCompletion(exception == null ? channel : null, exception);
      }
    }
  }
}
