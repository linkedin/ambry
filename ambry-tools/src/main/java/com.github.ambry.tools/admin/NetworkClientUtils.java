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
package com.github.ambry.tools.admin;

import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to send requests and receive responses via {@link NetworkClient}.
 * Each caller to {@link #poll(List)} will register a future (and may be a callback) for every request that needs to be
 * sent to the network client. On receiving the response, the {@link NetworkClientUtils} sets the result/exception in
 * the future and calls the callback if not {@code null}
 */
public class NetworkClientUtils {
  private final NetworkClient networkClient;
  private final int pollTimeMs;
  private final Map<Integer, RequestMetadata> correlationIdToRequestMetadataMap;
  private final Lock lock;
  private final NetworkClientPoller _networkClientPoller;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public NetworkClientUtils(NetworkClient networkClient, int pollTimeMs, long intervalToPollNetworkClientInMs) {
    this.networkClient = networkClient;
    this.pollTimeMs = pollTimeMs;
    this.correlationIdToRequestMetadataMap = new HashMap<>();
    this.lock = new ReentrantLock();
    this._networkClientPoller = new NetworkClientPoller(intervalToPollNetworkClientInMs);
    new Thread(_networkClientPoller).start();
  }

  /**
   * Polls the {@link NetworkClient} for the list of {@link RequestInfo} sent it
   * @param requestMetadataList
   */
  void poll(List<RequestMetadata> requestMetadataList) {
    try {
      lock.lock();
      List<RequestInfo> requestInfoList = new ArrayList<>();
      for (RequestMetadata requestMetadata : requestMetadataList) {
        requestInfoList.add(requestMetadata._requestInfo);
        int correlationId = requestMetadata.correlationId;
        if (!correlationIdToRequestMetadataMap.containsKey(correlationId)) {
          correlationIdToRequestMetadataMap.put(correlationId, requestMetadata);
        } else {
          logger.error("Duplicate request found for correlationId " + correlationId + ", requestMetadata "
              + requestMetadata._requestInfo);
        }
        List<ResponseInfo> responseInfos = networkClient.sendAndPoll(requestInfoList, pollTimeMs);
        onResponse(responseInfos);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Shuts down the {@link NetworkClientPoller} thread and the {@link NetworkClient}
   * @throws InterruptedException
   */
  void close()
      throws InterruptedException {
    _networkClientPoller.shutdown();
    networkClient.close();
  }

  /**
   * Holds metadata about {@link com.github.ambry.network.RequestInfo} like the associated correlationID and the
   * {@link FutureResult}
   * @param <T> the type of the response expected for the request
   */
  public static class RequestMetadata<T> {
    RequestInfo _requestInfo;
    FutureResult<T> futureResult;
    Callback<T> callback;
    int correlationId;
  }

  /**
   * Thread that polls the {@link NetworkClient} for new responses
   */
  class NetworkClientPoller implements Runnable {
    private boolean shutdown = false;
    private final long sleepTimeInMs;

    public NetworkClientPoller(long sleepTimeInMs) {
      this.sleepTimeInMs = sleepTimeInMs;
    }

    public void run() {
      while (!shutdown) {
        try {
          lock.lock();
          List<RequestInfo> requestInfoList = new ArrayList<>();
          List<ResponseInfo> responseInfos = networkClient.sendAndPoll(requestInfoList, pollTimeMs);
          onResponse(responseInfos);
        } finally {
          lock.unlock();
        }
        try {
          Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
          e.printStackTrace();
          shutdown = true;
        }
      }
    }

    void shutdown() {
      this.shutdown = true;
    }
  }

  /**
   * Handle the response from polling the {@link NetworkClient}.
   * @param responseInfoList the list of {@link ResponseInfo} containing the responses.
   */
  private void onResponse(List<ResponseInfo> responseInfoList) {
    for (ResponseInfo responseInfo : responseInfoList) {
      try {
        RequestOrResponseType type = ((RequestOrResponse) responseInfo.getRequestInfo().getRequest()).getRequestType();
        int correlationId = -1;
        switch (type) {
          case PutRequest:
            correlationId = ((PutRequest) responseInfo.getRequestInfo().getRequest()).getCorrelationId();
            break;
          case GetRequest:
            correlationId = ((GetRequest) responseInfo.getRequestInfo().getRequest()).getCorrelationId();
            break;
          case DeleteRequest:
            correlationId = ((DeleteRequest) responseInfo.getRequestInfo().getRequest()).getCorrelationId();
            break;
          default:
            logger.error("Unexpected response type: " + type + " received, discarding " + responseInfo);
        }
        if (correlationId != -1) {
          RequestMetadata requestMetadata = correlationIdToRequestMetadataMap.remove(correlationId);
          requestMetadata.futureResult.done(responseInfo.getResponse(),
              responseInfo.getError() != null ? new NetworkClientException("The Operation could not be completed.",
                  responseInfo.getError()) : null);
          if (requestMetadata.callback != null) {
            requestMetadata.callback.onCompletion(responseInfo.getResponse(),
                responseInfo.getError() != null ? new NetworkClientException("The Operation could not be completed.",
                    responseInfo.getError()) : null);
          }
        }
      } catch (Exception e) {
        logger.error("Unexpected error received while handling a response : " + responseInfo + ", exception : "
            + e.getStackTrace());
      }
    }
  }

  /**
   * Exception to hold the error thrown from the {@link NetworkClient}
   */
  static class NetworkClientException extends Exception {

    private NetworkClientErrorCode errorCode;

    public NetworkClientException(String msg, NetworkClientErrorCode errorCode) {
      super(msg);
      this.errorCode = errorCode;
    }
  }
}
