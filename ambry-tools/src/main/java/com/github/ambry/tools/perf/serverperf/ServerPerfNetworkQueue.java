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
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClient;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.utils.Time;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
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
public class ServerPerfNetworkQueue extends Thread implements Closeable {

  public static class ShutDownException extends Exception {
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

  private final Map<Integer, Long> pendingCorrelationIdToStartTimeMs = new LinkedHashMap<>();
  private final Map<Integer, Integer> pendingCorrelationIdToNetworkClientIdx = new HashMap<>();
  private final Map<Integer, RequestInfo> pendingCorrelationIdToRequestInfo = new HashMap<>();

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
      throw new ShutDownException();
    }
    requestInfos.offer(requestInfo);
  }

  /**
   * Checks and processes if any responses are in the queue
   * @param responseInfoProcessor processor
   * @throws Exception
   */
  void poll(ResponseInfoProcessor responseInfoProcessor) throws Exception {
    ResponseInfo responseInfo = responseInfos.poll();

    if (isShutDown && requestInfos.isEmpty() && responseInfos.isEmpty()
        && pendingCorrelationIdToStartTimeMs.isEmpty()) {
      throw new ShutDownException();
    }

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
   * Polls the request queue and submits the requests to network clients
   * in round robin way
   * Polls all network clients for responses that have arrived and
   * adds these to the queue
   *
   * It exits when shutdown is triggered and there are no pending requests
   *
   * 1. Collects the timed out requests and submits to corresponding network clients
   * 2. Polls all network clients for any responses that are available
   * 3. Polls the front of {@link #requestInfos} and submits to one network client in round robin way and adds the submitted
   *    correlation id to {@link #pendingCorrelationIdToStartTimeMs}{@link #pendingCorrelationIdToNetworkClientIdx}
   *    {@link #pendingCorrelationIdToRequestInfo}
   *  4. Removes all the collected responses from pending correlation id maps and adds to {@link #responseInfos}
   *
   *
   *  If shutdown is triggered , waits for all responses to be processed and shuts down  {@link #executorService}
   */
  @Override
  public void run() {
    while (!isShutDown || !requestInfos.isEmpty() || !pendingCorrelationIdToStartTimeMs.isEmpty()) {
      List<ResponseInfo> responseInfos = new ArrayList<>();

      Iterator<Map.Entry<Integer, Long>> pendingRequestIterator =
          pendingCorrelationIdToStartTimeMs.entrySet().iterator();

      while (pendingRequestIterator.hasNext()) {
        Map.Entry<Integer, Long> entry = pendingRequestIterator.next();
        int correlationId = entry.getKey();
        long startTimeMs = entry.getValue();

        long currentTimeInMs = time.milliseconds();

        if (startTimeMs + operationsTimeOutMs < currentTimeInMs) {

          responseInfos.add(new ResponseInfo(pendingCorrelationIdToRequestInfo.get(correlationId),
              NetworkClientErrorCode.TimeoutError, null));

          responseInfos.addAll(networkClients.get(pendingCorrelationIdToNetworkClientIdx.get(correlationId))
              .sendAndPoll(Collections.emptyList(), new HashSet<>(Collections.singletonList(correlationId)),
                  pollTimeout));
          pendingRequestIterator.remove();
        } else {
          break;
        }
      }

      networkClients.forEach(networkClient -> {
        responseInfos.addAll(networkClient.sendAndPoll(Collections.emptyList(), new HashSet<>(), pollTimeout));
      });

      if (!requestInfos.isEmpty()) {
        RequestInfo requestInfo = requestInfos.peek();

        pendingCorrelationIdToStartTimeMs.put(requestInfo.getRequest().getCorrelationId(), time.milliseconds());
        pendingCorrelationIdToNetworkClientIdx.put(requestInfo.getRequest().getCorrelationId(), clientIndex);
        pendingCorrelationIdToRequestInfo.put(requestInfo.getRequest().getCorrelationId(), requestInfo);
        requestInfos.remove();

        List<ResponseInfo> responses = networkClients.get(clientIndex)
            .sendAndPoll(Collections.singletonList(requestInfo), new HashSet<>(), pollTimeout);
        clientIndex++;
        clientIndex = clientIndex % networkClients.size();
        responseInfos.addAll(responses);
      }

      this.responseInfos.addAll(responseInfos);

      responseInfos.forEach(responseInfo -> {
        pendingCorrelationIdToStartTimeMs.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
        pendingCorrelationIdToNetworkClientIdx.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
        pendingCorrelationIdToRequestInfo.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
      });
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
    isShutDown = true;
    shutDownLatch.await();
  }

  @Override
  public void close() throws IOException {
    networkClients.forEach(Http2NetworkClient::close);
  }
}
