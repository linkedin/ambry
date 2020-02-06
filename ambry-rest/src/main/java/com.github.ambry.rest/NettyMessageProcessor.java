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

import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Inbound request handler for Netty.
 * <p/>
 * It processes a request (in parts) by converting it from Netty specific objects ({@link HttpObject},
 * {@link HttpRequest}, {@link HttpContent}) into {@link RestRequest}, a generic request object that all the RESTful
 * layers can understand and passes it down the pipeline to a {@link RestRequestService} through a
 * {@link RestRequestHandler}.
 * <p/>
 * It also maintains three pieces of state: -
 * 1. {@link RestRequest} representing the request - This is required since {@link HttpContent} that arrives
 * subsequently has to be available for reading through the read operations of {@link RestRequest}. This is a Netty
 * specific implementation, {@link NettyRequest}.
 * 2. {@link RestRequestHandler} that is used to handle requests - This is optional but the same instance of
 * {@link RestRequestHandler} is used for the lifetime of the channel.
 * 3. {@link RestResponseChannel} - An abstraction of the network channel that the underlying layers can use to reply to
 * the client. This has to maintained at least per request in order to inform the client of early processing exceptions.
 * (exceptions that occur before the request is handed off to the {@link RestRequestHandler}). This is a Netty specific
 * implementation, ({@link NettyResponseChannel}).
 * <p/>
 * Every time a new channel is created, Netty creates instances of all handlers in its pipeline. Since this class is one
 * of the handlers, a new instance of it is created for every connection. Therefore there can be multiple instances of
 * this class at any point of time.
 * <p/>
 * If there is no keepalive, a channel is created and destroyed for the lifetime of exactly one request. If there is
 * keepalive, requests can follow one after the other. But at any point of time, only one request is actually "alive"
 * in the channel (i.e. there cannot be multiple requests in flight that are being actively served on the same channel).
 */
public class NettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  private final NettyMetrics nettyMetrics;
  private final NettyConfig nettyConfig;
  private final PerformanceConfig performanceConfig;
  private final RestRequestHandler requestHandler;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // variables that will live through the life of the channel.
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private ChannelHandlerContext ctx = null;
  private final AtomicBoolean firstMessageReceived = new AtomicBoolean(false);
  private long channelActiveTimeMs;

  // variables that will live for the life of a single request.
  private volatile NettyRequest request = null;
  private volatile NettyResponseChannel responseChannel = null;
  private volatile boolean requestContentFullyReceived = false;

  // variables that live for one channelRead0
  private volatile Long lastChannelReadTime = null;

  /**
   * Creates a new NettyMessageProcessor instance with a metrics tracking object, a configuration object and a
   * {@code requestHandler}.
   * @param nettyMetrics the metrics object to use.
   * @param nettyConfig the configuration object to use.
   * @param performanceConfig the configuration object to use for SLO evaluation.
   * @param requestHandler the {@link RestRequestHandler} that can be used to submit requests that need to be handled.
   */
  public NettyMessageProcessor(NettyMetrics nettyMetrics, NettyConfig nettyConfig, PerformanceConfig performanceConfig,
      RestRequestHandler requestHandler) {
    this.nettyMetrics = nettyMetrics;
    this.nettyConfig = nettyConfig;
    this.performanceConfig = performanceConfig;
    this.requestHandler = requestHandler;
    logger.trace("Instantiated NettyMessageProcessor");
  }

  /**
   * Netty calls this function when the channel is active.
   * <p/>
   * This is called exactly once in the lifetime of the channel. If there is no keepalive, this will be called
   * before the request (and the channel lives to serve exactly one request). If there is keepalive, this will be
   * called just once before receiving the first request.
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   */
  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    channelActiveTimeMs = System.currentTimeMillis();
    logger.trace("Channel {} active", ctx.channel());
    this.ctx = ctx;
    nettyMetrics.channelCreationRate.mark();
  }

  /**
   * Netty calls this function when channel becomes inactive. The channel becomes inactive AFTER it is closed (either by
   * the server or the remote end). Any tasks that are performed at this point in time cannot communicate with the
   * client.
   * <p/>
   * This is called exactly once in the lifetime of the channel. If there is no keepalive, this will be called
   * after one request (since the channel lives to serve exactly one request). If there is keepalive, this will be
   * called once all the requests are done (the channel is closed).
   * <p/>
   * At this point we can perform state cleanup.
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    logger.trace("Channel {} inactive", ctx.channel());
    nettyMetrics.channelDestructionRate.mark();
    if (request != null && request.isOpen()) {
      logger.error("Request {} was aborted because the channel {} became inactive", request.getUri(), ctx.channel());
      onRequestAborted(Utils.convertToClientTerminationException(new ClosedChannelException()));
    } else {
      close();
    }
  }

  /**
   * Netty calls this function when any exception is caught during the functioning of this handler.
   * <p/>
   * Centralized error handling based on the exception is performed here. Error responses are sent to the client via
   * the {@link RestResponseChannel} wherever possible.
   * <p/>
   * If this function throws an Exception, it is bubbled up to the handler before this one in the Netty pipeline.
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param cause The cause of the error.
   * @throws Exception if there is an {@link Exception} while handling the {@code cause} caught.
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    try {
      if (request != null && request.isOpen() && cause instanceof Exception) {
        nettyMetrics.processorExceptionCaughtCount.inc();
        onRequestAborted((Exception) cause);
      } else if (isOpen()) {
        if (cause instanceof RestServiceException) {
          nettyMetrics.processorRestServiceExceptionCount.inc();
          RestServiceErrorCode errorCode = ((RestServiceException) cause).getErrorCode();
          if (ResponseStatus.getResponseStatus(errorCode) == ResponseStatus.BadRequest) {
            logger.debug("Swallowing error on channel {}", ctx.channel(), cause);
          } else {
            logger.error("Swallowing error on channel {}", ctx.channel(), cause);
          }
        } else if (cause instanceof IOException) {
          // for this case, it is certain that the server hasn't made any mistakes and the exception is probably
          // due to the client closing the connection. Therefore this is logged at the DEBUG level.
          nettyMetrics.processorIOExceptionCount.inc();
          logger.debug("Swallowing error on channel {}", ctx.channel(), cause);
        } else if (cause instanceof Exception) {
          nettyMetrics.processorUnknownExceptionCount.inc();
          logger.error("Swallowing error on channel {}", ctx.channel(), cause);
        } else {
          nettyMetrics.processorThrowableCount.inc();
          ctx.fireExceptionCaught(cause);
        }
        close();
      } else {
        nettyMetrics.processorErrorAfterCloseCount.inc();
        logger.debug("Caught error on channel {} after it was closed", ctx.channel(), cause);
      }
    } catch (Exception e) {
      String uri = (request != null) ? request.getUri() : null;
      nettyMetrics.exceptionCaughtTasksError.inc();
      logger.error("Swallowing exception during exceptionCaught tasks on channel {} for request {}", ctx.channel(), uri,
          e);
      close();
    }
  }

  /**
   * Netty calls this function when events that we have registered for, occur (in this case we are specifically waiting
   * for {@link IdleStateEvent} so that we close connections that have been idle too long - maybe due to client failure)
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param event The event that occurred.
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
    // NOTE: This is specifically in place to handle connections that close unexpectedly from the client side.
    // Even in that situation, any cleanup code that we have in the handlers will have to be called.
    if (event instanceof IdleStateEvent && ((IdleStateEvent) event).state() == IdleState.ALL_IDLE) {
      logger.info("Channel {} has been idle for {} seconds. Closing it", ctx.channel(),
          nettyConfig.nettyServerIdleTimeSeconds);
      nettyMetrics.idleConnectionCloseCount.inc();
      if (request != null && request.isOpen()) {
        onRequestAborted(Utils.convertToClientTerminationException(new ClosedChannelException()));
      } else {
        close();
      }
    }
  }

  /**
   * Netty calls this function whenever data is available on the channel that can be read.
   * <p/>
   * {@link HttpRequest} is converted to {@link NettyRequest} and passed to the the {@link RestRequestHandler}.
   * <p/>
   * {@link HttpContent} is added to the {@link NettyRequest} currently being served by the channel.
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param obj The {@link HttpObject} that forms a part of a request.
   * @throws RestServiceException if there is an error handling the processing of the current {@link HttpObject}.
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj) throws RestServiceException {
    if (isOpen()) {
      logger.trace("Reading on channel {}", ctx.channel());
      long currentTime = System.currentTimeMillis();
      if (firstMessageReceived.compareAndSet(false, true)) {
        if (ctx.pipeline().get(SslHandler.class) != null) {
          nettyMetrics.sslChannelActiveToFirstMessageReceiveTimeInMs.update(currentTime - channelActiveTimeMs);
        } else {
          nettyMetrics.channelActiveToFirstMessageReceiveTimeInMs.update(currentTime - channelActiveTimeMs);
        }
      }
      boolean recognized = false;
      boolean success = true;
      if (obj instanceof HttpRequest) {
        recognized = true;
        success = handleRequest((HttpRequest) obj);
      }
      // this is an if and not an else-if because a HttpObject can be both HttpRequest and HttpContent.
      if (success && obj instanceof HttpContent) {
        recognized = true;
        success = handleContent((HttpContent) obj);
      }
      if (success && !recognized) {
        logger.warn("Received null/unrecognized HttpObject {} on channel {}", obj, ctx.channel());
        nettyMetrics.unknownHttpObjectError.inc();
        if (responseChannel == null || requestContentFullyReceived) {
          resetState();
        }
        onRequestAborted(new RestServiceException("HttpObject received is null or not of a known type",
            RestServiceErrorCode.MalformedRequest));
      }
      if (lastChannelReadTime != null) {
        nettyMetrics.channelReadIntervalInMs.update(currentTime - lastChannelReadTime);
        logger.trace("Delay between channel reads is {} ms for channel {}", (currentTime - lastChannelReadTime),
            ctx.channel());
      }
      lastChannelReadTime = currentTime;
    } else {
      logger.debug("Read on channel {} ignored because it is inactive", ctx.channel());
    }
  }

  /**
   * Handles a {@link HttpRequest}.
   * <p/>
   * Does some state maintenance for all HTTP methods by creating a {@link RestRequest} wrapping this
   * {@link HttpRequest}
   * <p/>
   * In case of POST, delegates handling of {@link RestRequest} to the {@link RestRequestHandler}.
   * @param httpRequest the {@link HttpRequest} that needs to be handled.
   * @return {@code true} if the handling succeeded without problems.
   * @throws RestServiceException if there is an error handling the current {@link HttpRequest}.
   */
  private boolean handleRequest(HttpRequest httpRequest) throws RestServiceException {
    boolean success = true;
    if (responseChannel == null || requestContentFullyReceived) {
      // Once all content associated with a request has been received, this channel is clear to receive new requests.
      // If the client sends a request without waiting for the response, it is possible to screw things up a little
      // but doing so would constitute an error and no proper client would do that.
      long processingStartTime = System.currentTimeMillis();
      resetState();
      nettyMetrics.requestArrivalRate.mark();
      if (!httpRequest.decoderResult().isSuccess()) {
        success = false;
        logger.warn("Decoder failed because of malformed request on channel {}", ctx.channel(),
            httpRequest.decoderResult().cause());
        nettyMetrics.malformedRequestError.inc();
        onRequestAborted(new RestServiceException("Decoder failed because of malformed request",
            RestServiceErrorCode.MalformedRequest));
      } else {
        try {
          // We need to maintain state about the request itself for the subsequent parts (if any) that come in. We will
          // attach content to the request as the content arrives.
          if ((HttpMethod.POST.equals(httpRequest.method()) || HttpMethod.PUT.equals(httpRequest.method()))
              && HttpPostRequestDecoder.isMultipart(httpRequest)) {
            nettyMetrics.multipartPostRequestRate.mark();
            request = new NettyMultipartRequest(httpRequest, ctx.channel(), nettyMetrics,
                nettyConfig.nettyBlacklistedQueryParams, nettyConfig.nettyMultipartPostMaxSizeBytes);
          } else {
            request =
                new NettyRequest(httpRequest, ctx.channel(), nettyMetrics, nettyConfig.nettyBlacklistedQueryParams);
          }
          responseChannel.setRequest(request);
          logger.trace("Channel {} now handling request {}", ctx.channel(), request.getUri());
          // We send POST that is not multipart for handling immediately since we expect valid content with it that will
          // be streamed in. In the case of POST that is multipart, all the content has to be received for Netty's
          // decoder and NettyMultipartRequest to work. So it is scheduled for handling when LastHttpContent is received.
          // With any other method that we support, we do not expect any valid content. LastHttpContent is a Netty thing.
          // So we wait for LastHttpContent (throw an error if we don't receive it or receive something else) and then
          // schedule the other methods for handling in handleContent().
          if ((request.getRestMethod().equals(RestMethod.POST) || request.getRestMethod().equals(RestMethod.PUT))
              && !HttpPostRequestDecoder.isMultipart(httpRequest)) {
            requestHandler.handleRequest(request, responseChannel);
          }
        } catch (RestServiceException e) {
          success = false;
          onRequestAborted(e);
        } finally {
          if (request != null) {
            request.getMetricsTracker().nioMetricsTracker.addToRequestProcessingTime(
                System.currentTimeMillis() - processingStartTime);
          }
        }
      }
    } else {
      // We have received a request when we were not expecting one. This shouldn't happen and the channel is closed
      // because it is in a bad state.
      success = false;
      logger.error("New request received when previous request is yet to be fully received on channel {}. Request under"
          + " processing: {}. Unexpected request: {}", ctx.channel(), request.getUri(), httpRequest.uri());
      nettyMetrics.duplicateRequestError.inc();
      onRequestAborted(new RestServiceException("Received request in the middle of another request",
          RestServiceErrorCode.BadRequest));
    }
    return success;
  }

  /**
   * Handles a {@link HttpContent}.
   * <p/>
   * Checks to see that a valid {@link RestRequest} is available so that the content can be pushed into the request.
   * <p/>
   * If the HTTP method for the request is something other than POST, delegates handling of {@link RestRequest} to the
   * {@link RestRequestHandler} when {@link LastHttpContent} is received.
   * @param httpContent the {@link HttpContent} that needs to be handled.
   * @return {@code true} if the handling succeeded without problems.
   * @throws RestServiceException if there is an error handling the current {@link HttpContent}.
   */
  private boolean handleContent(HttpContent httpContent) throws RestServiceException {
    boolean success = true;
    if (request != null && !requestContentFullyReceived) {
      long processingStartTime = System.currentTimeMillis();
      nettyMetrics.bytesReadRate.mark(httpContent.content().readableBytes());
      requestContentFullyReceived = httpContent instanceof LastHttpContent;
      logger.trace("Received content for request {} on channel {}", request.getUri(), ctx.channel());
      try {
        request.addContent(httpContent);
      } catch (IllegalStateException e) {
        success = false;
        nettyMetrics.contentAdditionError.inc();
        onRequestAborted(new RestServiceException(e, RestServiceErrorCode.InvalidRequestState));
      } finally {
        long chunkProcessingTime = System.currentTimeMillis() - processingStartTime;
        nettyMetrics.requestChunkProcessingTimeInMs.update(chunkProcessingTime);
        request.getMetricsTracker().nioMetricsTracker.addToRequestProcessingTime(chunkProcessingTime);
      }
      if (success && (
          (!request.getRestMethod().equals(RestMethod.POST) && !request.getRestMethod().equals(RestMethod.PUT)) || (
              request.isMultipart() && requestContentFullyReceived))) {
        requestHandler.handleRequest(request, responseChannel);
      }
    } else {
      success = false;
      resetState();
      logger.warn("Received content when it was not expected on channel {}", ctx.channel());
      nettyMetrics.noRequestError.inc();
      onRequestAborted(
          new RestServiceException("Received content without a request", RestServiceErrorCode.InvalidRequestState));
    }
    return success;
  }

  /**
   * Resets the state of the processor in preparation for the next request.
   */
  private void resetState() {
    request = null;
    lastChannelReadTime = null;
    requestContentFullyReceived = false;
    responseChannel = new NettyResponseChannel(ctx, nettyMetrics, performanceConfig);
    logger.trace("Refreshed state for channel {}", ctx.channel());
  }

  /**
   * Aborts the request and sets state to indicate that the channel is no longer usable.
   * @param exception the {@link Exception} to send to the client if possible.
   */
  private void onRequestAborted(Exception exception) {
    if (responseChannel != null) {
      logger.trace("Aborting request on channel {}", ctx.channel());
      responseChannel.close(exception);
      channelOpen.set(false);
    } else {
      logger.debug("No response channel available for channel {}. Closing it immediately", ctx.channel());
      close();
    }
  }

  /**
   * Indicates whether the channel backing this handler is usable.
   * @return {@code true} if the channel backing this handler is usable, {@code false} otherwise.
   */
  private boolean isOpen() {
    return channelOpen.get() && ctx.channel().isActive();
  }

  /**
   * Closes the channel and marks it as unusable.
   */
  private void close() {
    logger.trace("Closing channel {}", ctx.channel());
    ctx.close();
    channelOpen.set(false);
  }
}
