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
package com.github.ambry.rest;

import com.github.ambry.commons.PerformanceIndex;
import com.github.ambry.commons.Thresholds;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GenericProgressiveFutureListener;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestResponseChannel} used to return responses via Netty. It is supported by
 * an underlying Netty channel whose handle this class has in the form of a {@link ChannelHandlerContext}.
 * <p/>
 * Data is sent in the order that threads call {@link #write(ByteBuf, Callback)}.
 * <p/>
 * If a write through this class fails at any time, the underlying channel will be closed immediately and no more writes
 * will be accepted and all scheduled writes will be notified of the failure.
 */
class NettyResponseChannel implements RestResponseChannel {
  // Detailed message about an error in an error response.
  static final String FAILURE_REASON_HEADER = "x-ambry-failure-reason";
  static final String ERROR_CODE_HEADER = "x-ambry-error-code";
  // add to this list if the connection needs to be closed on certain errors on GET, DELETE and HEAD.
  // for a POST or PUT, we always close the connection on error because we expect the channel to be in a bad state.
  static final List<HttpResponseStatus> CLOSE_CONNECTION_ERROR_STATUSES = new ArrayList<>();

  private final ChannelHandlerContext ctx;
  private final NettyMetrics nettyMetrics;
  private final ChannelProgressivePromise writeFuture;
  private final ChunkedWriteHandler chunkedWriteHandler;
  private final PerformanceConfig perfConfig;

  private static final Logger logger = LoggerFactory.getLogger(NettyResponseChannel.class);
  private final HttpResponse responseMetadata = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  // tracks whether onResponseComplete() has been called. Helps make it idempotent and also treats response channel as
  // closed if this is true.
  private final AtomicBoolean responseCompleteCalled = new AtomicBoolean(false);
  // tracks whether response metadata write has been initiated. Rejects any more attempts at writing metadata after this
  // has been set to true.
  private final AtomicBoolean responseMetadataWriteInitiated = new AtomicBoolean(false);
  private final AtomicLong totalBytesReceived = new AtomicLong(0);
  private final Queue<Chunk> chunksToWrite = new ConcurrentLinkedQueue<Chunk>();
  private final Queue<Chunk> chunksAwaitingCallback = new ConcurrentLinkedQueue<Chunk>();
  private final AtomicLong chunksToWriteCount = new AtomicLong(0);

  private NettyRequest request = null;
  // marked as true if force close is required because close() was called.
  private volatile boolean forceClose = false;
  // the ResponseStatus that will be sent (or has been sent) as a part of the response.
  private volatile ResponseStatus responseStatus = ResponseStatus.Ok;
  // the response metadata that was actually sent.
  private volatile HttpResponse finalResponseMetadata = null;
  // temp variable to hold the error response status which will be overwritten on responseStatus if the error response
  // was successfully sent
  private ResponseStatus errorResponseStatus = null;

  /**
   * Create an instance of NettyResponseChannel that will use {@code ctx} to return responses.
   * @param ctx the {@link ChannelHandlerContext} to use.
   * @param nettyMetrics the {@link NettyMetrics} instance to use.
   * @param performanceConfig the configuration object to use for performance evaluation.
   */
  NettyResponseChannel(ChannelHandlerContext ctx, NettyMetrics nettyMetrics, PerformanceConfig performanceConfig) {
    this.ctx = ctx;
    this.nettyMetrics = nettyMetrics;
    this.perfConfig = performanceConfig;
    chunkedWriteHandler = ctx.pipeline().get(ChunkedWriteHandler.class);
    writeFuture = ctx.newProgressivePromise();
    logger.trace("Instantiated NettyResponseChannel");
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    FutureResult<Long> futureResult = new FutureResult<>();
    write(Unpooled.wrappedBuffer(src), new Callback<Long>() {
      @Override
      public void onCompletion(Long result, Exception exception) {
        long r = result == null ? src.position() : result.longValue();
        src.position((int) r);
        futureResult.done(r, exception);
        if (callback != null) {
          callback.onCompletion(result, exception);
        }
      }
    });
    return futureResult;
  }

  @Override
  public Future<Long> write(ByteBuf src, Callback<Long> callback) {
    long writeProcessingStartTime = System.currentTimeMillis();
    if (!responseMetadataWriteInitiated.get()) {
      maybeWriteResponseMetadata(responseMetadata, new ResponseMetadataWriteListener());
    }
    if (finalResponseMetadata == null) {
      // If finalResponseMetadata is still null, it indicates channel becomes inactive.
      if (ctx.channel().isActive()) {
        logger.warn("Channel should be inactive status. {}", ctx.channel());
        nettyMetrics.channelStatusInconsistentCount.inc();
      } else {
        logger.debug("Scheduling a chunk cleanup on channel {} because response channel is closed.", ctx.channel());
        writeFuture.addListener(new CleanupCallback(new ClosedChannelException()));
      }
      FutureResult<Long> future = new FutureResult<Long>();
      future.done(0L, new ClosedChannelException());
      if (callback != null) {
        callback.onCompletion(0L, new ClosedChannelException());
      }
      return future;
    }
    Chunk chunk = new Chunk(src, callback);
    logger.debug("Netty Response Chunk size: " + src.readableBytes());
    chunksToWrite.add(chunk);
    if (HttpUtil.isContentLengthSet(finalResponseMetadata) && totalBytesReceived.get() > HttpUtil.getContentLength(
        finalResponseMetadata)) {
      Exception exception = new IllegalStateException(
          "Size of provided content [" + totalBytesReceived.get() + "] is greater than Content-Length set ["
              + HttpUtil.getContentLength(finalResponseMetadata) + "]");
      if (!writeFuture.isDone()) {
        writeFuture.setFailure(exception);
      }
      writeFuture.addListener(new CleanupCallback(exception));
    } else if (!isOpen()) {
      // the isOpen() check is not before addition to the queue because chunks need to be acknowledged in the order
      // they were received. If we don't add it to the queue and clean up, chunks may be acknowledged out of order.
      logger.debug("Scheduling a chunk cleanup on channel {} because response channel is closed", ctx.channel());
      writeFuture.addListener(new CleanupCallback(new ClosedChannelException()));
    } else if (finalResponseMetadata instanceof FullHttpResponse) {
      logger.debug("Scheduling a chunk cleanup on channel {} because Content-Length is 0", ctx.channel());
      Exception exception = null;
      // this is only allowed to be a 0 sized buffer.
      if (src.readableBytes() > 0) {
        exception = new IllegalStateException("Provided non zero size content after setting Content-Length to 0");
        if (!writeFuture.isDone()) {
          writeFuture.setFailure(exception);
        }
      }
      writeFuture.addListener(new CleanupCallback(exception));
    } else {
      chunkedWriteHandler.resumeTransfer();
    }

    long writeProcessingTime = System.currentTimeMillis() - writeProcessingStartTime;
    nettyMetrics.writeProcessingTimeInMs.update(writeProcessingTime);
    if (request != null) {
      request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(writeProcessingTime);
    }
    return chunk.future;
  }

  @Override
  public boolean isOpen() {
    return !responseCompleteCalled.get() && ctx.channel().isActive();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Marks the channel as closed. No further communication will be possible. Any pending writes (that are not already
   * flushed) might be discarded. The process of closing the network channel is also initiated.
   * <p/>
   * The underlying network channel might not be closed immediately but no more writes will be accepted and any calls to
   * {@link #isOpen()} after a call to this function will return {@code false}.
   */
  @Override
  public void close() {
    close(new ClosedChannelException());
  }

  @Override
  public void onResponseComplete(Exception exception) {
    long responseCompleteStartTime = System.currentTimeMillis();
    try {
      if (responseCompleteCalled.compareAndSet(false, true)) {
        logger.trace("Finished responding to current request on channel {}", ctx.channel());
        nettyMetrics.requestCompletionRate.mark();
        if (exception == null) {
          if (!maybeWriteResponseMetadata(responseMetadata, new ResponseMetadataWriteListener())) {
            // There were other writes. Let ChunkedWriteHandler finish if it has been kicked off.
            chunkedWriteHandler.resumeTransfer();
          }
        } else {
          log(exception);
          if (request != null) {
            request.getMetricsTracker().markFailure();
          }
          // need to set writeFuture as failed in case writes have started or chunks have been queued.
          if (!writeFuture.isDone()) {
            writeFuture.setFailure(exception);
          }
          if (!maybeSendErrorResponse(exception)) {
            completeRequest(true);
          }
        }
      } else if (exception != null) {
        // this is probably an attempt to force close the channel *after* the response is already complete.
        log(exception);
        if (!writeFuture.isDone()) {
          writeFuture.setFailure(exception);
        }
        completeRequest(true);
      }
      long responseFinishProcessingTime = System.currentTimeMillis() - responseCompleteStartTime;
      nettyMetrics.responseFinishProcessingTimeInMs.update(responseFinishProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(responseFinishProcessingTime);
      }
    } catch (Exception e) {
      logger.error("Swallowing exception encountered during onResponseComplete tasks", e);
      nettyMetrics.responseCompleteTasksError.inc();
      if (!writeFuture.isDone()) {
        writeFuture.setFailure(exception);
      }
      completeRequest(true);
    }
  }

  @Override
  public void setStatus(ResponseStatus status) throws RestServiceException {
    responseMetadata.setStatus(getHttpResponseStatus(status));
    responseStatus = status;
    logger.trace("Set status to {} for response on channel {}", responseMetadata.status(), ctx.channel());
  }

  @Override
  public ResponseStatus getStatus() {
    return responseStatus;
  }

  @Override
  public void setHeader(String headerName, Object headerValue) {
    if (headerName != null && headerValue != null) {
      long startTime = System.currentTimeMillis();
      responseMetadata.headers().set(headerName, headerValue);
      if (responseMetadataWriteInitiated.get()) {
        nettyMetrics.deadResponseAccessError.inc();
        throw new IllegalStateException(
            "Response metadata changed after it has already been written to the channel. Channel Active: "
                + ctx.channel().isActive());
      } else {
        logger.trace("Header {} set to {} for channel {}", headerName, responseMetadata.headers().get(headerName),
            ctx.channel());
        nettyMetrics.headerSetTimeInMs.update(System.currentTimeMillis() - startTime);
      }
    } else {
      throw new IllegalArgumentException("Header name [" + headerName + "] or header value [" + headerValue + "] null");
    }
  }

  @Override
  public Object getHeader(String headerName) {
    HttpResponse response = finalResponseMetadata;
    if (response == null) {
      response = responseMetadata;
    }
    return response.headers().get(headerName);
  }

  /**
   * Sets the request whose response is being served through this instance of NettyResponseChannel.
   * @param request the {@link NettyRequest} whose response is being served through this instance of
   *                NettyResponseChannel.
   */
  void setRequest(NettyRequest request) {
    if (request != null) {
      if (this.request == null) {
        this.request = request;
        HttpUtil.setKeepAlive(responseMetadata, request.isKeepAlive());
      } else {
        throw new IllegalStateException(
            "Request has already been set inside NettyResponseChannel for channel {} " + ctx.channel());
      }
    } else {
      throw new IllegalArgumentException("RestRequest provided is null");
    }
  }

  /**
   * Sends a response to the client based on the {@code exception} and marks the channel as closed. The process of
   * closing the network channel is also initiated.
   * <p/>
   * Functionality is the same as {@link #onResponseComplete(Exception)} except that the channel is always closed after
   * the response sending is complete.
   * @param exception the {@link Exception} to use while constructing an error message for the client.
   */
  void close(Exception exception) {
    forceClose = true;
    onResponseComplete(exception);
  }

  /**
   * Closes the request associated with this NettyResponseChannel.
   */
  private void closeRequest() {
    if (request != null && request.isOpen()) {
      request.close();
      evaluatePerformanceAndUpdateMetrics();
    }
  }

  /**
   * Evaluate performance of the request based on its associated {@link PerformanceIndex} and then update the metrics accordingly.
   */
  private void evaluatePerformanceAndUpdateMetrics() {
    RestRequestMetricsTracker restRequestMetricsTracker = request.getMetricsTracker();
    long roundTripTimeInMs = restRequestMetricsTracker.getRoundTripTimeInMs();
    long timeToFirstBytesInMs = restRequestMetricsTracker.getTimeToFirstByteInMs();
    long totalOutboundBytes = totalBytesReceived.get();
    long totalInboundBytes = request.getBytesReceived();
    RestMethod method = request.getRestMethod();
    Map<PerformanceIndex, Long> requestPerfToCheck = new HashMap<>();
    if (errorResponseStatus != null) {
      responseStatus = errorResponseStatus;
    }
    restRequestMetricsTracker.setResponseStatus(responseStatus);
    boolean shouldSkipCheck = false;
    switch (method) {
      case GET:
        if (responseStatus.isSuccess()) {
          requestPerfToCheck.put(PerformanceIndex.TimeToFirstByte, timeToFirstBytesInMs);
          requestPerfToCheck.put(PerformanceIndex.AverageBandwidth,
              roundTripTimeInMs == 0L ? Long.MAX_VALUE : totalOutboundBytes * 1000L / roundTripTimeInMs);
        } else if (responseStatus.isClientError() || responseStatus.isRedirection()) {
          requestPerfToCheck.put(PerformanceIndex.RoundTripTime, roundTripTimeInMs);
        }
        break;
      case POST:
        if (responseStatus.isSuccess()) {
          requestPerfToCheck.put(PerformanceIndex.AverageBandwidth,
              roundTripTimeInMs == 0L ? Long.MAX_VALUE : totalInboundBytes * 1000L / roundTripTimeInMs);
        } else if (responseStatus.isClientError() || responseStatus.isRedirection()) {
          requestPerfToCheck.put(PerformanceIndex.RoundTripTime, roundTripTimeInMs);
        }
        break;
      case PUT:
      case HEAD:
      case DELETE:
        if (!responseStatus.isServerError()) {
          requestPerfToCheck.put(PerformanceIndex.RoundTripTime, roundTripTimeInMs);
        }
        break;
      default:
        shouldSkipCheck = true;
        logger.debug("Temporarily no evaluation criteria for {} request. Mark as satisfied directly", method);
    }
    EnumMap<RestMethod, Thresholds> thresholdsByMethod =
        responseStatus.isSuccess() ? perfConfig.successRequestThresholds : perfConfig.nonSuccessRequestThresholds;
    if (!shouldSkipCheck && (responseStatus.isServerError() || !thresholdsByMethod.get(method)
        .checkThresholds(requestPerfToCheck))) {
      // this means either response is 5xx or request missed one of thresholds, the request should be unsatisfied
      restRequestMetricsTracker.markUnsatisfied();
      logUnsatisfiedRequest(requestPerfToCheck);
    }
    restRequestMetricsTracker.recordMetrics();
  }

  /**
   * Log unsatisfied request (print out request/response info and performance indices)
   * @param requestPerfToCheck the performance indices associated with this request.
   */
  private void logUnsatisfiedRequest(Map<PerformanceIndex, Long> requestPerfToCheck) {
    StringBuilder sb = new StringBuilder();
    sb.append("Unsatisfied request: uri=").append(request.getUri()).append("; method=").append(request.getRestMethod());
    if (request.getRestMethod() == RestMethod.POST) {
      sb.append("; location=").append((String) getHeader(RestUtils.Headers.LOCATION));
    }
    sb.append("; status=").append(responseStatus.getStatusCode());
    Object blobSize = getHeader(RestUtils.Headers.BLOB_SIZE);
    if (blobSize != null) {
      sb.append("; blob size=").append((String) blobSize);
    }
    for (Map.Entry<PerformanceIndex, Long> entry : requestPerfToCheck.entrySet()) {
      sb.append("; ").append(entry.getKey().toString()).append("=").append(entry.getValue());
      if (entry.getKey() == PerformanceIndex.AverageBandwidth) {
        sb.append("bytes/sec");
      } else {
        sb.append("ms");
      }
    }
    logger.warn(sb.toString());
  }

  /**
   * Writes response metadata to the channel if not already written previously and channel is active.
   * @param responseMetadata the {@link HttpResponse} that needs to be written.
   * @param listener the {@link GenericFutureListener} that needs to be attached to the write.
   * @return {@code true} if response metadata was written to the channel in this call. {@code false} otherwise.
   */
  private boolean maybeWriteResponseMetadata(HttpResponse responseMetadata,
      GenericFutureListener<ChannelFuture> listener) {
    long writeProcessingStartTime = System.currentTimeMillis();
    boolean writtenThisTime = false;
    if (ctx.channel().isActive() && responseMetadataWriteInitiated.compareAndSet(false, true)) {
      // we do some manipulation here for chunking. According to the HTTP spec, we can have either a Content-Length
      // or Transfer-Encoding:chunked, never both. So we check for Content-Length - if it is not there, we add
      // Transfer-Encoding:chunked on 200 response. Note that sending HttpContent chunks data anyway - we are just
      // explicitly specifying this in the header.
      if (!HttpUtil.isContentLengthSet(responseMetadata) && (responseMetadata.status().equals(HttpResponseStatus.OK)
          || responseMetadata.status().equals(HttpResponseStatus.PARTIAL_CONTENT))) {
        // This makes sure that we don't stomp on any existing transfer-encoding.
        HttpUtil.setTransferEncodingChunked(responseMetadata, true);
      } else if (HttpUtil.isContentLengthSet(responseMetadata) && HttpUtil.getContentLength(responseMetadata) == 0
          && !(responseMetadata instanceof FullHttpResponse)) {
        // if the Content-Length is 0, we can send a FullHttpResponse since there is no content expected.
        FullHttpResponse fullHttpResponse =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, responseMetadata.status());
        fullHttpResponse.headers().set(responseMetadata.headers());
        responseMetadata = fullHttpResponse;
      }
      logger.trace("Sending response with status {} on channel {}", responseMetadata.status(), ctx.channel());
      finalResponseMetadata = responseMetadata;
      ChannelPromise writePromise = ctx.newPromise().addListener(listener);
      ctx.writeAndFlush(responseMetadata, writePromise);
      writtenThisTime = true;
      long writeProcessingTime = System.currentTimeMillis() - writeProcessingStartTime;
      nettyMetrics.responseMetadataProcessingTimeInMs.update(writeProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.markFirstByteSent();
      }
    }
    return writtenThisTime;
  }

  /**
   * Builds and sends an error response to the client based on {@code cause}.
   * @param exception the cause of the request handling failure.
   * @return {@code true} if error response was scheduled to be sent. {@code false} otherwise.
   */
  private boolean maybeSendErrorResponse(Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    boolean responseSent = false;
    logger.trace("Sending error response to client on channel {}", ctx.channel());
    FullHttpResponse errorResponse = getErrorResponse(exception);
    if (maybeWriteResponseMetadata(errorResponse, new ErrorResponseWriteListener())) {
      logger.trace("Scheduled error response sending on channel {}", ctx.channel());
      responseStatus = errorResponseStatus;
      responseSent = true;
      long processingTime = System.currentTimeMillis() - processingStartTime;
      nettyMetrics.errorResponseProcessingTimeInMs.update(processingTime);
    } else {
      logger.debug("Could not send error response on channel {}", ctx.channel());
    }
    return responseSent;
  }

  /**
   * Provided a cause, returns an error response with the right status and error message.
   * @param cause the cause of the error.
   * @return a {@link FullHttpResponse} with the error message that can be sent to the client.
   */
  private FullHttpResponse getErrorResponse(Throwable cause) {
    HttpResponseStatus status;
    RestServiceErrorCode restServiceErrorCode = null;
    String errReason = null;
    if (cause instanceof RestServiceException) {
      RestServiceException restServiceException = (RestServiceException) cause;
      restServiceErrorCode = restServiceException.getErrorCode();
      errorResponseStatus = ResponseStatus.getResponseStatus(restServiceErrorCode);
      status = getHttpResponseStatus(errorResponseStatus);
      if (status == HttpResponseStatus.BAD_REQUEST || restServiceException.shouldIncludeExceptionMessageInResponse()) {
        errReason = new String(
            Utils.getRootCause(cause).getMessage().replaceAll("[\n\t\r]", " ").getBytes(StandardCharsets.US_ASCII),
            StandardCharsets.US_ASCII);
      }
    } else if (Utils.isPossibleClientTermination(cause)) {
      nettyMetrics.clientEarlyTerminationCount.inc();
      status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      errorResponseStatus = ResponseStatus.InternalServerError;
    } else {
      nettyMetrics.internalServerErrorCount.inc();
      status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      errorResponseStatus = ResponseStatus.InternalServerError;
    }
    logger.trace("Constructed error response for the client - [{} - {}]", status, errReason);
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
    response.headers().set(HttpHeaderNames.DATE, new GregorianCalendar().getTime());
    HttpUtil.setContentLength(response, 0);
    if (errReason != null) {
      response.headers().set(FAILURE_REASON_HEADER, errReason);
    }
    if (restServiceErrorCode != null && HttpStatusClass.CLIENT_ERROR.contains(status.code())) {
      response.headers().set(ERROR_CODE_HEADER, restServiceErrorCode.name());
    }
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
    // if there is an ALLOW header in the response so far constructed, copy it
    if (responseMetadata.headers().contains(HttpHeaderNames.ALLOW)) {
      response.headers().set(HttpHeaderNames.ALLOW, responseMetadata.headers().get(HttpHeaderNames.ALLOW));
    } else if (errorResponseStatus == ResponseStatus.MethodNotAllowed) {
      logger.warn("Response is {} but there is no value for {}", ResponseStatus.MethodNotAllowed,
          HttpHeaderNames.ALLOW);
    }
    copyTrackingHeaders(responseMetadata, response);
    HttpUtil.setKeepAlive(response, shouldKeepAlive(status));
    return response;
  }

  /**
   * Copy tracking headers (if any) from sourceResponse to targetResponse.
   * @param sourceResponse the source response with the tracking headers.
   * @param targetResponse the target response that the tracking headers will be copied over.
   */
  private void copyTrackingHeaders(HttpResponse sourceResponse, HttpResponse targetResponse) {
    for (String header : RestUtils.TrackingHeaders.TRACKING_HEADERS) {
      if (sourceResponse.headers().contains(header)) {
        targetResponse.headers().set(header, sourceResponse.headers().get(header));
      }
    }
  }

  /**
   * @param status the {@link HttpResponseStatus}
   * @return {@code true} if the channel should be kept alive
   */
  private boolean shouldKeepAlive(HttpResponseStatus status) {
    boolean shouldKeepAlive = true;
    if (request != null) {
      if (request.getArgs().containsKey(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT)) {
        shouldKeepAlive =
            Boolean.parseBoolean(request.getArgs().get(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT).toString());
      } else if (request.getRestMethod().equals(RestMethod.POST)) {
        shouldKeepAlive = false;
      } else if (request.getRestMethod().equals(RestMethod.PUT) && (HttpUtil.isTransferEncodingChunked(request.request)
          || HttpUtil.getContentLength(request.request, 0L) > 0)) {
        shouldKeepAlive = false;
      }
    }
    return shouldKeepAlive && !forceClose && HttpUtil.isKeepAlive(responseMetadata)
        && !CLOSE_CONNECTION_ERROR_STATUSES.contains(status);
  }

  /**
   * Converts a {@link ResponseStatus} into a {@link HttpResponseStatus}.
   * @param responseStatus {@link ResponseStatus} that needs to be mapped to a {@link HttpResponseStatus}.
   * @return the {@link HttpResponseStatus} that maps to the {@link ResponseStatus}.
   */
  private HttpResponseStatus getHttpResponseStatus(ResponseStatus responseStatus) {
    HttpResponseStatus status;
    switch (responseStatus) {
      case Ok:
        nettyMetrics.okCount.inc();
        status = HttpResponseStatus.OK;
        break;
      case Created:
        nettyMetrics.createdCount.inc();
        status = HttpResponseStatus.CREATED;
        break;
      case Accepted:
        nettyMetrics.acceptedCount.inc();
        status = HttpResponseStatus.ACCEPTED;
        break;
      case PartialContent:
        nettyMetrics.partialContentCount.inc();
        status = HttpResponseStatus.PARTIAL_CONTENT;
        break;
      case NotModified:
        nettyMetrics.notModifiedCount.inc();
        status = HttpResponseStatus.NOT_MODIFIED;
        break;
      case BadRequest:
        nettyMetrics.badRequestCount.inc();
        status = HttpResponseStatus.BAD_REQUEST;
        break;
      case Unauthorized:
        nettyMetrics.unauthorizedCount.inc();
        status = HttpResponseStatus.UNAUTHORIZED;
        break;
      case NotFound:
        nettyMetrics.notFoundCount.inc();
        status = HttpResponseStatus.NOT_FOUND;
        break;
      case Gone:
        nettyMetrics.goneCount.inc();
        status = HttpResponseStatus.GONE;
        break;
      case Forbidden:
        nettyMetrics.forbiddenCount.inc();
        status = HttpResponseStatus.FORBIDDEN;
        break;
      case ProxyAuthenticationRequired:
        nettyMetrics.proxyAuthRequiredCount.inc();
        status = HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED;
        break;
      case RangeNotSatisfiable:
        nettyMetrics.rangeNotSatisfiableCount.inc();
        status = HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
        break;
      case TooManyRequests:
        nettyMetrics.tooManyRequests.inc();
        status = HttpResponseStatus.TOO_MANY_REQUESTS;
        break;
      case RequestTooLarge:
        nettyMetrics.requestTooLargeCount.inc();
        status = HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
        break;
      case InternalServerError:
        nettyMetrics.internalServerErrorCount.inc();
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        break;
      case ServiceUnavailable:
        nettyMetrics.serviceUnavailableErrorCount.inc();
        status = HttpResponseStatus.SERVICE_UNAVAILABLE;
        break;
      case InsufficientCapacity:
        nettyMetrics.insufficientCapacityErrorCount.inc();
        status = HttpResponseStatus.INSUFFICIENT_STORAGE;
        break;
      case PreconditionFailed:
        nettyMetrics.preconditionFailedErrorCount.inc();
        status = HttpResponseStatus.PRECONDITION_FAILED;
        break;
      case MethodNotAllowed:
        nettyMetrics.methodNotAllowedErrorCount.inc();
        status = HttpResponseStatus.METHOD_NOT_ALLOWED;
        break;
      default:
        nettyMetrics.unknownResponseStatusCount.inc();
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        break;
    }
    return status;
  }

  /**
   * Completes the request by closing the request and network channel (if {@code closeNetworkChannel} is {@code true}).
   * </p>
   * May also close the channel if the class internally is forcing a close (i.e. if {@link #close()} is called.
   * @param closeNetworkChannel network channel is closed if {@code true}.
   */
  private void completeRequest(boolean closeNetworkChannel) {
    if ((closeNetworkChannel || forceClose) && ctx.channel().isOpen()) {
      writeFuture.addListener(ChannelFutureListener.CLOSE);
      logger.trace("Requested closing of channel {}", ctx.channel());
    }
    closeRequest();
  }

  /**
   * Handles post-mortem of writes that have failed.
   * @param cause the cause of the failure.
   * @param propagateErrorIfRequired if {@code true} and {@code cause} is not an instance of {@link Exception}, the
   *                                 error is propagated through the netty pipeline.
   */
  private void handleChannelWriteFailure(Throwable cause, boolean propagateErrorIfRequired) {
    long writeFailureProcessingStartTime = System.currentTimeMillis();
    try {
      nettyMetrics.channelWriteError.inc();
      Exception exception;
      if (!(cause instanceof Exception)) {
        logger.warn("Encountered a throwable on channel write failure", cause);
        exception = new IllegalStateException("Encountered a Throwable - " + cause.getMessage());
        if (propagateErrorIfRequired) {
          // we can't ignore throwables - so we let Netty deal with it.
          ctx.fireExceptionCaught(cause);
          nettyMetrics.throwableCount.inc();
        }
      } else if (cause instanceof ClosedChannelException) {
        // wrap the exception in something we recognize as a client termination
        exception = Utils.convertToClientTerminationException(cause);
      } else {
        exception = (Exception) cause;
      }
      onResponseComplete(exception);
      cleanupChunks(exception);
    } finally {
      nettyMetrics.channelWriteFailureProcessingTimeInMs.update(
          System.currentTimeMillis() - writeFailureProcessingStartTime);
    }
  }

  /**
   * Logs the exception at the appropriate level.
   * @param exception the {@link Exception} that has to be logged.
   */
  private void log(Exception exception) {
    if (ctx.channel().isActive()) {
      String uri = "unknown";
      RestMethod restMethod = RestMethod.UNKNOWN;
      if (request != null) {
        uri = request.getUri();
        restMethod = request.getRestMethod();
      }
      if (exception instanceof RestServiceException) {
        RestServiceErrorCode errorCode = ((RestServiceException) exception).getErrorCode();
        ResponseStatus responseStatus = ResponseStatus.getResponseStatus(errorCode);
        if (responseStatus == ResponseStatus.InternalServerError) {
          logger.error("Internal error handling request {} with method {}", uri, restMethod, exception);
        } else {
          logger.trace("Error handling request {} with method {}", uri, restMethod, exception);
        }
      } else if (Utils.isPossibleClientTermination(exception)) {
        logger.trace("Client likely terminated connection while handling request {} with method {}", uri, restMethod,
            exception);
      } else {
        logger.error("Unexpected error handling request {} with method {}", uri, restMethod, exception);
      }
    } else {
      logger.debug("Exception encountered after channel {} became inactive", ctx.channel(), exception);
    }
  }

  /**
   * Cleans up all the chunks remaining by invoking their callbacks.
   * @param exception the {@link Exception} to provide in the callback. Can be {@code null}.
   */
  private void cleanupChunks(Exception exception) {
    logger.trace("Cleaning up remaining chunks");
    Chunk chunk = chunksAwaitingCallback.poll();
    while (chunk != null) {
      chunk.resolveChunk(exception);
      chunk = chunksAwaitingCallback.poll();
    }
    chunk = chunksToWrite.poll();
    while (chunk != null) {
      chunk.onDequeue();
      chunk.resolveChunk(exception);
      chunk = chunksToWrite.poll();
    }
  }

  // helper classes

  /**
   * Class that represents a chunk of data.
   */
  private class Chunk {
    /**
     * The future that will be set on chunk resolution.
     */
    final FutureResult<Long> future = new FutureResult<Long>();
    /**
     * The bytes associated with this chunk.
     */
    final ByteBuf buffer;
    /**
     * The number of bytes that will need to be written.
     */
    final long bytesToBeWritten;
    /**
     * If progress in {@link #writeFuture} becomes greater than this number, the future/callback will be triggered.
     */
    final long writeCompleteThreshold;
    /**
     * Information on whether this is the last chunk.
     * <p/>
     * This value will be {@code true} for at most one chunk. If {@link HttpHeaderNames#CONTENT_LENGTH} is not set,
     * there will be no chunks with this value as {@code true}.
     */
    final boolean isLast;
    private final Callback<Long> callback;
    private final long chunkQueueStartTime = System.currentTimeMillis();

    private long chunkWriteStartTime;

    /**
     * Creates a chunk.
     * @param buffer the {@link ByteBuf} that forms the data of this chunk.
     * @param callback the {@link Callback} to invoke when {@link #writeCompleteThreshold} is reached.
     */
    Chunk(ByteBuf buffer, Callback<Long> callback) {
      this.buffer = buffer;
      bytesToBeWritten = buffer.readableBytes();
      this.callback = callback;
      writeCompleteThreshold = totalBytesReceived.addAndGet(bytesToBeWritten);
      chunksToWriteCount.incrementAndGet();

      // if channel becomes inactive, no one set finalResponseMetadata. It's possible finalResponseMetadata is null.
      long contentLength = finalResponseMetadata == null ? -1 : HttpUtil.getContentLength(finalResponseMetadata, -1);
      isLast = contentLength != -1 && writeCompleteThreshold >= contentLength;
    }

    /**
     * Does tasks (like tracking) that need to be done when a chunk is dequeued for processing.
     */
    void onDequeue() {
      chunksToWriteCount.decrementAndGet();
      chunkWriteStartTime = System.currentTimeMillis();
      long chunkQueueTime = chunkWriteStartTime - chunkQueueStartTime;
      nettyMetrics.chunkQueueTimeInMs.update(chunkQueueTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(chunkQueueTime);
      }
    }

    /**
     * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
     * resolved, the data inside it is considered void.
     * @param exception the reason for chunk handling failure.
     */
    void resolveChunk(Exception exception) {
      long chunkWriteFinishTime = System.currentTimeMillis();
      long bytesWritten = 0;
      if (exception == null) {
        bytesWritten = bytesToBeWritten;
        buffer.skipBytes((int) bytesWritten);
      }
      nettyMetrics.bytesWriteRate.mark(bytesWritten);
      future.done(bytesWritten, exception);
      if (callback != null) {
        callback.onCompletion(bytesWritten, exception);
      }
      long chunkResolutionProcessingTime = System.currentTimeMillis() - chunkWriteFinishTime;
      long chunkWriteTime = chunkWriteFinishTime - chunkWriteStartTime;
      nettyMetrics.channelWriteTimeInMs.update(chunkWriteTime);
      nettyMetrics.chunkResolutionProcessingTimeInMs.update(chunkResolutionProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(
            chunkWriteTime + chunkResolutionProcessingTime);
      }
    }
  }

  /**
   * Dispenses chunks when asked to by the {@link ChunkedWriteHandler}.
   */
  private class ChunkDispenser implements ChunkedInput<HttpContent> {
    // NOTE: Due to a bug in HttpChunkedInput, some code there has been reproduced here instead of using it directly.
    private boolean sentLastChunk = false;
    private final AtomicLong progress = new AtomicLong(0);

    /**
     * Determines if input has ended by examining response state and number of chunks pending for write.
     * @return {@code true} if there are no more chunks to write and the end marker has been sent. {@code false}
     *         otherwise.
     */
    @Override
    public boolean isEndOfInput() {
      return sentLastChunk;
    }

    @Override
    public void close() {
      // nothing to do.
    }

    /**
     * Dispenses the next chunk from {@link #chunksToWrite} if one is available and adds the chunk dispensed to
     * {@link #chunksAwaitingCallback}.
     * @param ctx the {@link ChannelHandlerContext} for the channel being written to.
     * @return a chunk of data if one is available. {@code null} otherwise.
     */
    @Override
    public HttpContent readChunk(ChannelHandlerContext ctx) throws Exception {
      return readChunk(ctx.alloc());
    }

    /**
     * Dispenses the next chunk from {@link #chunksToWrite} if one is available and adds the chunk dispensed to
     * {@link #chunksAwaitingCallback}.
     * @param allocator the {@link ByteBufAllocator} to use if required.
     * @return a chunk of data if one is available. {@code null} otherwise.
     */
    @Override
    public HttpContent readChunk(ByteBufAllocator allocator) throws Exception {
      long chunkDispenseStartTime = System.currentTimeMillis();
      logger.trace("Servicing request for next chunk on channel {}", ctx.channel());
      HttpContent content = null;
      Chunk chunk = chunksToWrite.poll();
      if (chunk != null) {
        chunk.onDequeue();
        progress.addAndGet(chunk.buffer.readableBytes());
        chunksAwaitingCallback.add(chunk);
        // Since netty would release the outbound message after finishing writing, here we have to increase the ref-counter.
        if (chunk.isLast) {
          content = new DefaultLastHttpContent(chunk.buffer.retainedDuplicate());
          sentLastChunk = true;
        } else {
          content = new DefaultHttpContent(chunk.buffer.retainedDuplicate());
        }
      } else if (allChunksWritten() && !sentLastChunk) {
        // Send last chunk for this input
        sentLastChunk = true;
        content = LastHttpContent.EMPTY_LAST_CONTENT;
        logger.trace("Last chunk was sent on channel {}", ctx.channel());
      }
      nettyMetrics.chunkDispenseTimeInMs.update(System.currentTimeMillis() - chunkDispenseStartTime);
      return content;
    }

    @Override
    public long length() {
      return HttpUtil.getContentLength(finalResponseMetadata, -1);
    }

    @Override
    public long progress() {
      return progress.get();
    }

    /**
     * Determines if input has ended by examining response state and number of chunks pending for write.
     * @return {@code true} if there are no more chunks to write. {@code false} otherwise.
     */
    private boolean allChunksWritten() {
      return responseCompleteCalled.get() && !writeFuture.isDone() && chunksToWriteCount.get() == 0;
    }
  }

  // callback classes

  /**
   * Invokes callback on any chunks that are eligible for callback.
   */
  private class CallbackInvoker implements GenericProgressiveFutureListener<ChannelProgressiveFuture> {

    /**
     * Uses {@code progress} to determine chunks whose callbacks need to be invoked.
     * @param future the {@link ChannelProgressiveFuture} that is being listened on.
     * @param progress the total number of bytes that have been written starting from the time writes were invoked via
     *                 {@link ChunkedWriteHandler}.
     * @param total the total number of bytes that need to be written i.e. the target number. This is not relevant to
     *              {@link ChunkedWriteHandler} because there is no target number. All calls to this function except
     *              the very last one will have a negative {@code total}.
     */
    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
      logger.trace("{} bytes of response written on channel {}", progress, ctx.channel());
      while (chunksAwaitingCallback.peek() != null
          && progress >= chunksAwaitingCallback.peek().writeCompleteThreshold) {
        chunksAwaitingCallback.poll().resolveChunk(null);
      }
    }

    /**
     * Called once the write is complete i.e. either all chunks that were needed to be written were written or there
     * was an error  writing the chunks.
     * @param future the {@link ChannelProgressiveFuture} that is being listened on.
     */
    @Override
    public void operationComplete(ChannelProgressiveFuture future) {
      if (future.isSuccess()) {
        logger.trace("Response sending complete on channel {}", ctx.channel());
        completeRequest(request == null || !request.isKeepAlive());
      } else {
        handleChannelWriteFailure(future.cause(), true);
      }
    }
  }

  /**
   * Callback for writes of response metadata.
   */
  private class ResponseMetadataWriteListener implements GenericFutureListener<ChannelFuture> {
    private final long responseWriteStartTime = System.currentTimeMillis();

    /**
     * If the operation completed successfully, a write via the {@link ChunkedWriteHandler} is initiated. Otherwise,
     * failure is handled.
     * @param future the {@link ChannelFuture} that is being listened on.
     */
    @Override
    public void operationComplete(ChannelFuture future) {
      long writeFinishTime = System.currentTimeMillis();
      if (future.isSuccess()) {
        if (finalResponseMetadata instanceof LastHttpContent) {
          // this is the case if finalResponseMetadata is a FullHttpResponse.
          // in this case there is nothing more to write.
          if (!writeFuture.isDone()) {
            writeFuture.setSuccess();
            completeRequest(!HttpUtil.isKeepAlive(finalResponseMetadata));
          }
        } else {
          // otherwise there is some content to write.
          logger.trace("Starting ChunkedWriteHandler on channel {}", ctx.channel());
          writeFuture.addListener(new CallbackInvoker());
          ctx.writeAndFlush(new ChunkDispenser(), writeFuture);
        }
      } else {
        handleChannelWriteFailure(future.cause(), true);
      }
      long responseAfterWriteProcessingTime = System.currentTimeMillis() - writeFinishTime;
      long channelWriteTime = writeFinishTime - responseWriteStartTime;
      nettyMetrics.channelWriteTimeInMs.update(channelWriteTime);
      nettyMetrics.responseMetadataAfterWriteProcessingTimeInMs.update(responseAfterWriteProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(
            channelWriteTime + responseAfterWriteProcessingTime);
      }
    }
  }

  /**
   * Listener that will be attached to the write of an error response.
   */
  private class ErrorResponseWriteListener implements GenericFutureListener<ChannelFuture> {
    private final long responseWriteStartTime = System.currentTimeMillis();

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      long writeFinishTime = System.currentTimeMillis();
      long channelWriteTime = writeFinishTime - responseWriteStartTime;
      if (future.isSuccess()) {
        completeRequest(!HttpUtil.isKeepAlive(finalResponseMetadata));
      } else {
        handleChannelWriteFailure(future.cause(), true);
      }
      long responseAfterWriteProcessingTime = System.currentTimeMillis() - writeFinishTime;
      nettyMetrics.channelWriteTimeInMs.update(channelWriteTime);
      nettyMetrics.responseMetadataAfterWriteProcessingTimeInMs.update(responseAfterWriteProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(
            channelWriteTime + responseAfterWriteProcessingTime);
      }
    }
  }

  /**
   * Cleans up chunks that are remaining. This serves two purposes:
   * 1. Guarding against the case where {@link CallbackInvoker#operationComplete(ChannelProgressiveFuture)} might have
   * finished already.
   * 2. Cleaning up any zero sized chunks when {@link HttpHeaderNames#CONTENT_LENGTH} is 0 and a
   * {@link FullHttpResponse} has been sent.
   */
  private class CleanupCallback implements GenericFutureListener<ChannelFuture> {
    private final Exception exception;

    /**
     * Instantiate a CleanupCallback with an exception to return once cleanup is activated.
     * @param exception the {@link Exception} to return as a part of the callback. Can be {@code null}. This can be
     *                  overriden if the channel write ended in failure.
     */
    CleanupCallback(Exception exception) {
      this.exception = exception;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      Throwable cause = future.cause() == null ? exception : future.cause();
      if (cause != null) {
        handleChannelWriteFailure(cause, false);
      } else {
        cleanupChunks(null);
      }
      logger.debug("Chunk cleanup complete on channel {}", ctx.channel());
    }
  }
}
