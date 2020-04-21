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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Perf specific implementation of {@link Router}.
 *
 * Get: Returns pre-populated repetitive data based on total size and chunk size configured.
 * GetBlobInfo: Returns pre-populated data.
 * PutBlob: Discards all bytes received.
 * DeleteBlob: No op.
 */
class PerfRouter implements Router {
  protected final static String BLOB_ID = "AAEAAQAAAAAAAAAAAAAAJGYwNWFkMDc4LWNlNGEtNDY3NS04N2RkLTllZjliMzNlYjYzOA";

  private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final Random random = new Random();

  private static RouterException ROUTER_CLOSED_EXCEPTION =
      new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);

  private final PerfRouterMetrics perfRouterMetrics;
  private final BlobProperties blobProperties;
  private final byte[] usermetadata;
  private final byte[] chunk;
  private volatile boolean routerOpen = true;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates an instance of PerfRouter with configuration as specified in {@code perfRouterConfig}.
   * @param perfConfig the {@link PerfConfig} to use that determines behavior.
   * @param perfRouterMetrics the {@link PerfRouterMetrics} instance to use to record metrics.
   */
  public PerfRouter(PerfConfig perfConfig, PerfRouterMetrics perfRouterMetrics) {
    this.perfRouterMetrics = perfRouterMetrics;
    blobProperties = new BlobProperties(perfConfig.perfBlobSize, "PerfRouter", Account.UNKNOWN_ACCOUNT_ID,
        Container.UNKNOWN_CONTAINER_ID, false);
    usermetadata = getRandomString(perfConfig.perfUserMetadataSize).getBytes();
    chunk = new byte[perfConfig.perfRouterChunkSize];
    random.nextBytes(chunk);
    logger.trace("Instantiated PerfRouter");
  }

  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options, Callback<GetBlobResult> callback) {
    logger.trace("Received getBlob call");
    FutureResult<GetBlobResult> futureResult = new FutureResult<>();
    if (!routerOpen) {
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      GetBlobResult result = null;
      switch (options.getOperationType()) {
        case All:
          result = new GetBlobResult(new BlobInfo(blobProperties, usermetadata),
              new PerfRSC(chunk, blobProperties.getBlobSize()));
          break;
        case Data:
          result = new GetBlobResult(null, new PerfRSC(chunk, blobProperties.getBlobSize()));
          break;
        case BlobInfo:
          result = new GetBlobResult(new BlobInfo(blobProperties, usermetadata), null);
          break;
      }
      completeOperation(futureResult, callback, result, null);
    }
    return futureResult;
  }

  /**
   * Consumes the data in {@code channel} and simply throws it away. {@code blobProperties} and {@code usermetadata} are
   * ignored.
   * @param blobProperties The properties of the blob.
   * @param usermetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param options the {@link PutBlobOptions} for the blob.
   * @param callback the {@link Callback} to invoke on operation completion.
   * @return a {@link Future} that will contain a (dummy) blob id.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, final ReadableStreamChannel channel,
      PutBlobOptions options, final Callback<String> callback) {
    logger.trace("Received putBlob call");
    final FutureResult<String> futureResult = new FutureResult<String>();
    if (!routerOpen) {
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      final long putConsumeStartTime = System.currentTimeMillis();
      channel.readInto(new NoOpAWC(), new Callback<Long>() {
        @Override
        public void onCompletion(Long result, Exception exception) {
          String operationResult = null;
          if (exception == null && (result == null || (channel.getSize() != -1 && result != channel.getSize()))) {
            exception = new IllegalStateException("The content was not completely read");
          } else if (exception == null) {
            logger.debug("Total bytes read - {}", result);
            perfRouterMetrics.putSizeInBytes.update(result);
            operationResult = PerfRouter.BLOB_ID;
          }
          perfRouterMetrics.putContentConsumeTimeInMs.update(System.currentTimeMillis() - putConsumeStartTime);
          completeOperation(futureResult, callback, operationResult, exception);
        }
      });
    }
    return futureResult;
  }

  @Override
  public Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      Callback<String> callback) {
    logger.trace("Received stitchBlob call");
    final FutureResult<String> futureResult = new FutureResult<>();
    if (!routerOpen) {
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      completeOperation(futureResult, callback, BLOB_ID, null);
    }
    return futureResult;
  }

  /**
   * Does nothing. Simply indicates success immediately.
   * @param blobId (ignored).
   * @param serviceId (ignored).
   * @param callback the {@link Callback} to invoke on operation completion.
   * @return a {@link FutureResult} that will eventually contain the result of the operation.
   */
  @Override
  public Future<Void> deleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    logger.trace("Received deleteBlob call");
    FutureResult<Void> futureResult = new FutureResult<Void>();
    if (!routerOpen) {
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      completeOperation(futureResult, callback, null, null);
    }
    return futureResult;
  }

  /**
   * Does nothing. Simply indicates success immediately.
   * @param blobId (ignored).
   * @param serviceId (ignored).
   * @param expiresAtMs (ignored).
   * @param callback the {@link Callback} to invoke on operation completion.
   * @return a {@link FutureResult} that will eventually contain the result of the operation.
   */
  @Override
  public Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs, Callback<Void> callback) {
    logger.trace("Received updateBlobTtl call");
    FutureResult<Void> futureResult = new FutureResult<Void>();
    if (!routerOpen) {
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      completeOperation(futureResult, callback, null, null);
    }
    return futureResult;
  }

  /**
   * Does nothing. Simply indicates success immediately.
   * @param blobId (ignored).
   * @param serviceId (ignored).
   * @param callback the {@link Callback} to invoke on operation completion.
   * @return a {@link FutureResult} that will eventually contain the result of the operation.
   */
  @Override
  public Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    logger.trace("Received undeleteBlob call");
    FutureResult<Void> futureResult = new FutureResult<Void>();
    if (!routerOpen) {
      completeOperation(futureResult, callback, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      completeOperation(futureResult, callback, null, null);
    }
    return futureResult;
  }

  @Override
  public void close() {
    routerOpen = false;
  }

  /**
   * Completes a router operation by invoking the {@code callback} and setting the {@code futureResult} with
   * {@code operationResult} (if any) and {@code exception} (if any).
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   * @param <T> the type of {@code futureResult}, {@code callback} and {@code operationResult}.
   */
  private <T> void completeOperation(FutureResult<T> futureResult, Callback<T> callback, T operationResult,
      Exception exception) {
    RuntimeException runtimeException = null;
    if (exception != null) {
      runtimeException = new RuntimeException(exception);
    }
    futureResult.done(operationResult, runtimeException);
    if (callback != null) {
      callback.onCompletion(operationResult, exception);
    }
  }

  /**
   * Gets a random string of size {@code length}.
   * @param length the size of the required random string.
   * @return a random string of size {@code length}.
   */
  private String getRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }
}
