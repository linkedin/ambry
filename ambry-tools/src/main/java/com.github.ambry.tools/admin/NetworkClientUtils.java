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
import java.util.concurrent.CountDownLatch;
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
public class NetworkClientUtils implements Runnable {
  private final NetworkClient networkClient;
  private final int pollTimeMs;
  private final Map<Integer, RequestMetadata> correlationIdToRequestMetadataMap;
  private final Lock lock;
  private boolean shutdown = false;
  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private volatile List<RequestInfo> requestInfoList = new ArrayList<>();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public NetworkClientUtils(NetworkClient networkClient, int pollTimeMs) {
    this.networkClient = networkClient;
    this.pollTimeMs = pollTimeMs;
    this.correlationIdToRequestMetadataMap = new HashMap<>();
    this.lock = new ReentrantLock();
    new Thread(this).start();
  }

  /**
   * Adds the list of {@link RequestMetadata} which has the {@link RequestInfo} to be sent to the {@link NetworkClient}
   * @param requestMetadataList
   */
  void poll(List<RequestMetadata> requestMetadataList) {
    try {
      lock.lock();
      for (RequestMetadata requestMetadata : requestMetadataList) {
        requestInfoList.add(requestMetadata.requestInfo);
        int correlationId = requestMetadata.correlationId;
        if (!correlationIdToRequestMetadataMap.containsKey(correlationId)) {
          correlationIdToRequestMetadataMap.put(correlationId, requestMetadata);
        } else {
          logger.error("Ignoring duplicate request found for correlationId " + correlationId + ", requestMetadata "
              + requestMetadata.requestInfo);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void run() {
    while (!shutdown) {
      lock.lock();
      List<RequestInfo> requestInfoListToSend = new ArrayList<>();
      requestInfoListToSend.addAll(requestInfoList);
      requestInfoList.clear();
      lock.unlock();
      List<ResponseInfo> responseInfos = networkClient.sendAndPoll(requestInfoListToSend, pollTimeMs);
      onResponse(responseInfos);
    }
    shutdownLatch.countDown();
  }

  /**
   * Shuts down the {@link NetworkClientUtils} and the {@link NetworkClient}
   * @throws InterruptedException
   */
  void close()
      throws InterruptedException {
    shutdown = true;
    shutdownLatch.await();
    networkClient.close();
  }

  /**
   * Holds metadata about {@link com.github.ambry.network.RequestInfo} like the associated correlationID and the
   * {@link FutureResult}
   * @param <T> the type of the response expected for the request
   */
  public static class RequestMetadata<T> {
    RequestInfo requestInfo;
    FutureResult<T> futureResult;
    Callback<T> callback;
    int correlationId;
  }

  /**
   * Handle the response from polling the {@link NetworkClient}.
   * @param responseInfoList the list of {@link ResponseInfo} containing the responses.
   */
  private void onResponse(List<ResponseInfo> responseInfoList) {
    for (ResponseInfo responseInfo : responseInfoList) {
      try {
        RequestOrResponse requestOrResponse = (RequestOrResponse) responseInfo.getRequestInfo().getRequest();
        int correlationId = requestOrResponse.getCorrelationId();
          RequestMetadata requestMetadata = correlationIdToRequestMetadataMap.remove(correlationId);
        if(requestMetadata != null) {
          requestMetadata.futureResult.done(responseInfo.getResponse(),
              responseInfo.getError() != null ? new NetworkClientException("The Operation could not be completed.",
                  responseInfo.getError()) : null);
          if (requestMetadata.callback != null) {
            requestMetadata.callback.onCompletion(responseInfo.getResponse(),
                responseInfo.getError() != null ? new NetworkClientException("The Operation could not be completed.",
                    responseInfo.getError()) : null);}

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
