/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.perf.serverperf;

import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClient;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Acts as a Bounded blocking queue for Network requests and response
 * Adds the submitted requests to a queue
 * Polls the submitted requests and submits to network clients
 * Polls the network clients and adds the responses to a queue
 * This response queue is used for polling and processing the responses
 * Makes sure that maximum {@link #maxParallelRequest} requests are getting processed.
 */
public class ServerPerfNetworkQueue extends Thread {

  private static class InflightNetworkClientRequest {
    private final long startTimeMs;
    private final RequestInfo requestInfo;

    InflightNetworkClientRequest(long startTimeMs, RequestInfo requestInfo) {
      this.startTimeMs = startTimeMs;
      this.requestInfo = requestInfo;
    }

    long getStartTimeMs() {
      return startTimeMs;
    }

    RequestInfo getRequestInfo() {
      return requestInfo;
    }

    int getCorrelationId() {
      return requestInfo.getRequest().getCorrelationId();
    }
  }

  private final List<Http2NetworkClient> networkClients;
  private int clientIndex;
  private final ConcurrentLinkedQueue<RequestInfo> requestInfos;
  private final ConcurrentLinkedQueue<ResponseInfo> responseInfos;
  private final int pollTimeout;
  private final int maxParallelism;

  private final Semaphore maxParallelRequest;
  private final ExecutorService executorService;

  private final CountDownLatch shutDownLatch;

  private final Time time;
  private boolean isShutDown;

  private final int operationsTimeOutMs;

  private final Map<Integer, LinkedHashSet<InflightNetworkClientRequest>> networkClientIdToInflightRequest;
  private static final Logger logger = LoggerFactory.getLogger(ServerPerfNetworkQueue.class);

  /**
   *
   * @param verifiableProperties properties
   * @param metrics http2 client metrics
   * @param time  ambry Time
   * @param maxParallelism maximum parallel requests that can be processed
   * @param clientCount total number of network clients to use
   * @param operationsTimeOutSec time for a request to mark as timed out
   * @throws Exception exception
   */
  ServerPerfNetworkQueue(VerifiableProperties verifiableProperties, Http2ClientMetrics metrics, Time time,
      int maxParallelism, int clientCount, int operationsTimeOutSec) throws Exception {
    SSLFactory sslFactory = new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    Http2ClientConfig http2ClientConfig = new Http2ClientConfig(verifiableProperties);
    Http2NetworkClientFactory networkClientFactory =
        new Http2NetworkClientFactory(metrics, http2ClientConfig, sslFactory, time);
    networkClients = new ArrayList<>();
    for (int i = 0; i < clientCount; i++) {
      networkClients.add(networkClientFactory.getNetworkClient());
    }
    this.time = time;
    isShutDown = false;
    clientIndex = 0;
    requestInfos = new ConcurrentLinkedQueue<>();
    responseInfos = new ConcurrentLinkedQueue<>();
    networkClientIdToInflightRequest = new HashMap<>();
    pollTimeout = 0;
    this.operationsTimeOutMs = operationsTimeOutSec * 1000;
    this.maxParallelism = maxParallelism;
    maxParallelRequest = new Semaphore(maxParallelism, true);
    executorService = Executors.newFixedThreadPool(maxParallelism);
    shutDownLatch = new CountDownLatch(1);
  }

  /**
   * Adds the request info to the queue and ready for polling
   * @param requestInfo request info
   * @throws Exception
   */
  void submit(RequestInfo requestInfo) throws Exception {
    if (isShutDown) {
      throw new ShutDownException();
    }
    maxParallelRequest.acquire();
    if (isShutDown) {
      maxParallelRequest.release();
      throw new ShutDownException();
    }
    requestInfos.offer(requestInfo);
  }

  private boolean networkClientInflightRequestsEmpty() {
    return networkClientIdToInflightRequest.values().stream().allMatch(Set::isEmpty);
  }

  /**
   * Checks and processes if any responses are in the queue
   * @param responseInfoProcessor processor
   * @throws Exception
   */
  void poll(ResponseInfoProcessor responseInfoProcessor) throws Exception {

    if (isShutDown && requestInfos.isEmpty() && responseInfos.isEmpty() && networkClientInflightRequestsEmpty()) {
      throw new ShutDownException();
    }

    ResponseInfo responseInfo = responseInfos.poll();
    if (responseInfo == null) {
      return;
    }

    executorService.submit(new Thread(() -> {
      try {
        responseInfoProcessor.process(responseInfo);
      } catch (Exception e) {
        logger.error("Encountered error while processing", e);
      } finally {
        maxParallelRequest.release();
      }
    }));
  }

  /**
   * This keeps running continuously
   *
   * Polls the request queue and submits the requests to network clients
   * in round robin way and responses that have arrived and
   * adds these to the queue
   *
   * It exits when shutdown is triggered and there are no pending requests
   *
   * 1. Picks a network client in round robin use {@link #clientIndex}
   * 2. Checks the requests that are timed out using {@link #networkClientIdToInflightRequest} and collects them to drop
   * 3. Polls the top of {@link #requestInfos} for submitting to network client
   * 4. Submits the requests and dropped requests to network client and collects responses
   * 5. Adds the sent requests to {@link #networkClientIdToInflightRequest}
   * 6. Removes the top of {@link #requestInfos} if earlier any request was polled.
   * 7. Removes the responses received from {@link #networkClientIdToInflightRequest}
   * 8. moves {@link #clientIndex} by 1 so, next client can be picked
   *
   *
   *  If shutdown is triggered , waits for all responses to be processed and shuts down  {@link #executorService}
   *  1. Releases a token so if any thread trying to submit is waiting stops waiting.
   *  2. Tries to acquire {@link #maxParallelism+1} tokens as it can only acquire all tokens when
   *      all responses get processed
   */
  @Override
  public void run() {
    while (!isShutDown || !requestInfos.isEmpty() || !networkClientInflightRequestsEmpty()) {

      Http2NetworkClient networkClient = networkClients.get(clientIndex);
      Set<Integer> correlationIdsToDrop = new HashSet<>();

      networkClientIdToInflightRequest.computeIfAbsent(clientIndex, (clientIndex) -> new LinkedHashSet<>());
      LinkedHashSet<InflightNetworkClientRequest> pendingNetworkClientRequest =
          networkClientIdToInflightRequest.get(clientIndex);

      for (InflightNetworkClientRequest request : pendingNetworkClientRequest) {
        long startTimeMs = request.getStartTimeMs();
        long currentTimeInMs = time.milliseconds();

        if (startTimeMs + operationsTimeOutMs < currentTimeInMs) {
          correlationIdsToDrop.add(request.getCorrelationId());
        } else {
          break;
        }
      }

      List<RequestInfo> requestInfosToSubmit = new ArrayList<>();
      RequestInfo requestInfo = requestInfos.peek();

      if (requestInfo != null) {
        requestInfosToSubmit.add(requestInfo);
      }

      List<ResponseInfo> responseInfos =
          networkClient.sendAndPoll(requestInfosToSubmit, correlationIdsToDrop, pollTimeout);

      networkClientIdToInflightRequest.get(clientIndex)
          .addAll(requestInfosToSubmit.stream()
              .map(r -> new InflightNetworkClientRequest(time.milliseconds(), r))
              .collect(Collectors.toList()));

      Set<Integer> receivedCorrelationIds = new HashSet<>();
      responseInfos.forEach(responseInfo -> {
        receivedCorrelationIds.add(responseInfo.getRequestInfo().getRequest().getCorrelationId());
      });

      this.responseInfos.addAll(responseInfos);
      networkClientIdToInflightRequest.get(clientIndex)
          .removeIf(request -> receivedCorrelationIds.contains(request.getCorrelationId()));

      if (requestInfo != null) {
        requestInfos.remove();
      }

      clientIndex++;
      clientIndex = clientIndex % networkClients.size();
    }

    try {
      maxParallelRequest.release(1);
      maxParallelRequest.acquire(maxParallelism + 1);
    } catch (InterruptedException e) {
      logger.warn("Caught exception while waiting for executor service to finish", e);
    } finally {
      executorService.shutdownNow();
      shutDownLatch.countDown();
    }
  }

  void shutDown() throws Exception {
    logger.info("Shutting down the network queue");
    isShutDown = true;
    shutDownLatch.await();

    networkClients.forEach(Http2NetworkClient::close);
    logger.info("Shutdown complete for network queue");
  }
}
