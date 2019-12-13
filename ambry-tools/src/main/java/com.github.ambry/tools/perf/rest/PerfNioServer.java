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
package com.github.ambry.tools.perf.rest;

import com.codahale.metrics.Snapshot;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestHandler;
import com.github.ambry.rest.RestRequestMetricsTracker;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Perf specific implementation of {@link NioServer}.
 *
 * Creates load for downstream based on configuration. Discards all received responses.
 */
class PerfNioServer implements NioServer {
  private final LoadCreator loadCreator;
  private final Thread loadCreatorThread;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates an instance of PerfNioServer.
   * @param perfConfig an instance of {@link PerfConfig} that determines behavior.
   * @param perfNioServerMetrics the {@link PerfNioServerMetrics} instance to use to record metrics.
   * @param restRequestHandler the instance of {@link RestRequestHandler} to use to handle requests.
   */
  protected PerfNioServer(PerfConfig perfConfig, PerfNioServerMetrics perfNioServerMetrics,
      RestRequestHandler restRequestHandler) {
    loadCreator = new LoadCreator(perfConfig, perfNioServerMetrics, restRequestHandler);
    loadCreatorThread = new Thread(loadCreator);
    logger.trace("Instantiated PerfNioServer");
  }

  @Override
  public void start() throws InstantiationException {
    logger.info("Starting PerfNioServer");
    loadCreatorThread.start();
    logger.info("Started PerfNioServer");
  }

  @Override
  public void shutdown() {
    logger.info("Shutting down PerfNioServer");
    try {
      if (loadCreator.shutdown(10, TimeUnit.SECONDS)) {
        logger.info("PerfNioServer shutdown complete");
      } else {
        logger.info("PerfNioServer shutdown did not complete");
      }
    } catch (InterruptedException e) {
      logger.error("PerfNioServer shutdown was interrupted", e);
    }
  }

  /**
   * Creates artificial load for a {@link RestRequestHandler} instance based on some configurable parameters.
   */
  private static class LoadCreator implements Runnable {
    private final PerfConfig perfConfig;
    private final PerfNioServerMetrics perfNioServerMetrics;
    private final RestRequestHandler restRequestHandler;
    private final String usermetadata;
    private final byte[] chunk;
    private final LinkedBlockingQueue<Boolean> slots;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile boolean running = true;

    /**
     * Creates an instance of LoadCreator;
     * @param perfConfig an instance of {@link PerfConfig} that determines behavior.
     * @param perfNioServerMetrics the {@link PerfNioServerMetrics} instance to use to record metrics.
     * @param restRequestHandler the instance of {@link RestRequestHandler} to use to handle requests.
     */
    protected LoadCreator(PerfConfig perfConfig, PerfNioServerMetrics perfNioServerMetrics,
        RestRequestHandler restRequestHandler) {
      this.perfConfig = perfConfig;
      this.perfNioServerMetrics = perfNioServerMetrics;
      this.restRequestHandler = restRequestHandler;
      chunk = new byte[perfConfig.perfNioServerChunkSize];
      byte[] umBytes = new byte[perfConfig.perfUserMetadataSize];
      Random random = new Random();
      random.nextBytes(chunk);
      random.nextBytes(umBytes);
      usermetadata = new String(umBytes);
      slots = new LinkedBlockingQueue<Boolean>(perfConfig.perfNioServerConcurrency);
      logger.trace("Instantiated LoadCreator");
    }

    @Override
    public void run() {
      init();
      Callback<Void> callback = new OperationCallback();
      long startTime = System.currentTimeMillis();
      long requestCount = 0;
      while (running) {
        try {
          slots.take();
          logger.trace("Slot available for new request");
          perfNioServerMetrics.requestRate.mark();
          requestCount++;
          RestRequest restRequest =
              new PerfRestRequest(perfConfig.perfRequestRestMethod, usermetadata, chunk, perfConfig.perfBlobSize);
          restRequest.getMetricsTracker().nioMetricsTracker.markRequestReceived();
          restRequestHandler.handleRequest(restRequest,
              new NoOpRestResponseChannel(restRequest, perfNioServerMetrics, callback));
        } catch (Exception e) {
          callback.onCompletion(null, e);
        }
      }
      long totalRunTimeInMs = System.currentTimeMillis() - startTime;
      logger.info("LoadCreator executed for approximately {} s and sent {} requests ({} requests/sec)",
          (float) totalRunTimeInMs / (float) Time.MsPerSec, requestCount,
          (float) requestCount * (float) Time.MsPerSec / (float) totalRunTimeInMs);
      Snapshot rttStatsSnapshot = perfNioServerMetrics.requestRoundTripTimeInMs.getSnapshot();
      logger.info("RTT stats: Min - {} ms, Mean - {} ms, Max - {} ms", rttStatsSnapshot.getMin(),
          rttStatsSnapshot.getMean(), rttStatsSnapshot.getMax());
      logger.info("RTT stats: 95th percentile - {} ms, 99th percentile - {} ms, 999th percentile - {} ms",
          rttStatsSnapshot.get95thPercentile(), rttStatsSnapshot.get99thPercentile(),
          rttStatsSnapshot.get999thPercentile());
      shutdownLatch.countDown();
    }

    /**
     * Marks that shutdown is required and waits for shutdown for the specified time.
     * @param timeout the amount of time to wait for shutdown.
     * @param timeUnit time unit of {@code timeout}.
     * @return {@code true} if shutdown succeeded within the {@code timeout}. {@code false} otherwise.
     * @throws InterruptedException if the wait for shutdown is interrupted.
     */
    protected boolean shutdown(long timeout, TimeUnit timeUnit) throws InterruptedException {
      logger.debug("Shutting down LoadCreator");
      running = false;
      return shutdownLatch.await(timeout, timeUnit);
    }

    /**
     * Initializes load creation by adding slots based on concurrency required.
     */
    private void init() {
      for (int i = 0; i < perfConfig.perfNioServerConcurrency; i++) {
        slots.add(true);
      }
      logger.info("Setup to maintain concurrency of {}", perfConfig.perfNioServerConcurrency);
    }

    /**
     * Callback for when response for a request is complete.
     */
    private class OperationCallback implements Callback<Void> {
      private final Logger logger = LoggerFactory.getLogger(getClass());

      /**
       * Reports exceptions if any and adds a slot so that it can be reused for another request.
       * @param result The result of the request. This would be non null when the request executed successfully
       * @param exception The exception that was reported on execution of the request
       */
      @Override
      public void onCompletion(Void result, Exception exception) {
        slots.add(true);
        logger.trace("Slot added");
      }
    }
  }

  /**
   * Perf test implementation of {@link RestRequest}.
   */
  private static class PerfRestRequest implements RestRequest {
    private static final String PERF_PATH = "perf-path";
    private static final String PERF_URI = "perf-uri";
    private static final String MULTIPLE_HEADER_VALUE_DELIMITER = ", ";

    private final RestMethod restMethod;
    private final ReadableStreamChannel readableStreamChannel;
    private final Map<String, Object> args;
    private final RestRequestMetricsTracker restRequestMetricsTracker = new RestRequestMetricsTracker();

    /**
     * Creates an instance of PerfRestRequest.
     * @param restMethod the {@link RestMethod} desired.
     * @param usermetadata the usermetadata that needs to be stored with the blob.
     * @param chunk the data that will form each chunk of this request upto the size defined in {@code totalBlobSize}.
     * @param totalBlobSize the total size of the blob represented by this PerfRestRequest.
     */
    public PerfRestRequest(RestMethod restMethod, String usermetadata, byte[] chunk, long totalBlobSize) {
      this.restMethod = restMethod;
      Map<String, Object> inFluxArgs = new HashMap<String, Object>();
      if (restMethod.equals(RestMethod.POST)) {
        readableStreamChannel = new PerfRSC(chunk, totalBlobSize);
        addValueForHeader(inFluxArgs, RestUtils.Headers.BLOB_SIZE, Long.toString(totalBlobSize));
        addValueForHeader(inFluxArgs, RestUtils.Headers.SERVICE_ID, "PerfNioServer");
        addValueForHeader(inFluxArgs, RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
        addValueForHeader(inFluxArgs, "x-um-perf-user-metadata", usermetadata);
      } else {
        readableStreamChannel = new PerfRSC(null, 0);
      }
      args = Collections.unmodifiableMap(inFluxArgs);
    }

    @Override
    public RestMethod getRestMethod() {
      return restMethod;
    }

    @Override
    public String getPath() {
      return PERF_PATH;
    }

    @Override
    public String getUri() {
      return PERF_URI;
    }

    @Override
    public Map<String, Object> getArgs() {
      return args;
    }

    @Override
    public Object setArg(String key, Object value) {
      return args.put(key, value);
    }

    @Override
    public SSLSession getSSLSession() {
      return null;
    }

    @Override
    public void prepare() {
      // no op.
    }

    @Override
    public boolean isOpen() {
      return readableStreamChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
      readableStreamChannel.close();
      restRequestMetricsTracker.nioMetricsTracker.markRequestCompleted();
      restRequestMetricsTracker.recordMetrics();
    }

    @Override
    public RestRequestMetricsTracker getMetricsTracker() {
      return restRequestMetricsTracker;
    }

    @Override
    public long getSize() {
      return readableStreamChannel.getSize();
    }

    @Override
    public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
      return readableStreamChannel.readInto(asyncWritableChannel, callback);
    }

    @Override
    public void setDigestAlgorithm(String digestAlgorithm) {
      // no op;
    }

    @Override
    public byte[] getDigest() {
      return null;
    }

    @Override
    public long getBytesReceived() {
      return 0;
    }

    /**
     * Adds a value for a header.
     * @param toAddTo the map to add the {@code key} with value {@code value} to.
     * @param key the key for which {@code value} is a value.
     * @param value a value of {@code key}
     */
    private void addValueForHeader(Map<String, Object> toAddTo, String key, Object value) {
      if (value != null && value instanceof String) {
        StringBuilder sb;
        if (toAddTo.get(key) == null) {
          sb = new StringBuilder(value.toString());
          toAddTo.put(key, sb);
        } else {
          sb = (StringBuilder) toAddTo.get(key);
          sb.append(MULTIPLE_HEADER_VALUE_DELIMITER).append(value);
        }
      } else if (value != null && toAddTo.containsKey(key)) {
        throw new IllegalStateException("Value of key [" + key + "] is not a string and it already exists in the args");
      } else {
        toAddTo.put(key, value);
      }
    }
  }

  /**
   * An implementation of {@link RestResponseChannel} that is a no-op on most operations. However a {@link Callback} can
   * be registered to be notified on {@link #onResponseComplete(Exception)} or {@link #close()}.
   */
  private static class NoOpRestResponseChannel implements RestResponseChannel {
    private final RestRequest restRequest;
    private final PerfNioServerMetrics perfNioServerMetrics;
    private final Callback<Void> callback;
    private final NoOpAWC noOpAWC = new NoOpAWC();
    private final long startTime = System.currentTimeMillis();
    private final AtomicBoolean responseComplete = new AtomicBoolean(false);
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, Object> headers = new HashMap<>();

    private ResponseStatus responseStatus = ResponseStatus.Ok;

    /**
     * Creates a new instance of NoOpRestResponseChannel.
     * @param restRequest the {@link RestRequest} for which a response will be sent over this channel.
     * @param perfNioServerMetrics the {@link PerfNioServerMetrics} instance to use to record metrics.
     * @param callback the {@link Callback} that will be invoked on {@link #onResponseComplete(Exception)} or
     * {@link #close()}.
     */
    public NoOpRestResponseChannel(RestRequest restRequest, PerfNioServerMetrics perfNioServerMetrics,
        Callback<Void> callback) {
      this.restRequest = restRequest;
      this.perfNioServerMetrics = perfNioServerMetrics;
      this.callback = callback;
    }

    @Override
    public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
      return noOpAWC.write(src, callback);
    }

    @Override
    public boolean isOpen() {
      return noOpAWC.isOpen();
    }

    @Override
    public void close() throws IOException {
      onResponseComplete(new ClosedChannelException());
    }

    @Override
    public void onResponseComplete(Exception exception) {
      if (responseComplete.compareAndSet(false, true)) {
        try {
          if (exception != null) {
            logger.error("Request handling encountered exception", exception);
            restRequest.getMetricsTracker().markFailure();
            perfNioServerMetrics.requestResponseError.inc();
          }
          restRequest.close();
          long requestRoundTripTime = System.currentTimeMillis() - startTime;
          logger.debug("Request took {} ms. {} bytes were written into the channel", requestRoundTripTime,
              noOpAWC.getBytesConsumedTillNow());
          perfNioServerMetrics.requestRoundTripTimeInMs.update(requestRoundTripTime);
          if (callback != null) {
            callback.onCompletion(null, exception);
          }
        } catch (IOException e) {
          logger.error("Exception during onResponseComplete", e);
        }
      }
    }

    @Override
    public void setStatus(ResponseStatus status) throws RestServiceException {
      responseStatus = status;
    }

    @Override
    public ResponseStatus getStatus() {
      return responseStatus;
    }

    @Override
    public void setHeader(String headerName, Object headerValue) {
      headers.put(headerName, headerValue);
    }

    @Override
    public Object getHeader(String headerName) {
      return headers.get(headerName);
    }
  }
}
