package com.github.ambry.rest;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.nio.channels.ClosedChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Inbound request handler for Netty.
 * <p/>
 * It processes a request (in parts) by converting it from Netty specific objects ({@link HttpObject},
 * {@link HttpRequest}, {@link HttpContent}) into {@link RestRequest}, a generic request object that all the RESTful
 * layers can understand and passes it down the pipeline to a {@link BlobStorageService} through a
 * {@link AsyncRequestResponseHandler}.
 * <p/>
 * It also maintains three pieces of state: -
 * 1. {@link RestRequest} representing the request - This is required since {@link HttpContent} that arrives
 * subsequently has to be available for reading through the read operations of {@link RestRequest}. This is a Netty
 * specific implementation, {@link NettyRequest}.
 * 2. {@link AsyncRequestResponseHandler} that is used to handle requests - This is optional but the same instance of
 * {@link AsyncRequestResponseHandler} is used for the lifetime of the channel.
 * 3. {@link RestResponseChannel} - An abstraction of the network channel that the underlying layers can use to reply to
 * the client. This has to maintained at least per request in order to inform the client of early processing exceptions.
 * (exceptions that occur before the request is handed off to the {@link AsyncRequestResponseHandler}). This is a Netty
 * specific implementation, ({@link NettyResponseChannel}).
 * <p/>
 * Every time a new channel is created, Netty creates instances of all handlers in its pipeline. Since this class is one
 * of the handlers, a new instance of it is created for every connection. Therefore there can be multiple instances of
 * this class at any point of time.
 * <p/>
 * If there is no keepalive, a channel is created and destroyed for the lifetime of exactly one request. If there is
 * keepalive, requests can follow one after the other. But at any point of time, only one request is actually "alive"
 * in the channel (i.e. there cannot be multiple requests in flight that are being actively served on the same channel).
 */
class NettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  private final NettyMetrics nettyMetrics;
  private final NettyConfig nettyConfig;
  private final AsyncRequestResponseHandler requestHandler;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // variables that will live through the life of the channel.
  private ChannelHandlerContext ctx = null;

  // variables that will live for the life of a single request.
  private volatile NettyRequest request = null;
  private volatile NettyResponseChannel responseChannel = null;

  // variables that live for one channelRead0
  private volatile Long lastChannelReadTime = null;

  /**
   * Creates a new NettyMessageProcessor instance with a metrics tracking object, a configuration object and a
   * {@code requestResponseHandlerController}.
   * @param nettyMetrics the metrics object to use.
   * @param nettyConfig the configuration object to use.
   * @param requestResponseHandlerController a {@link RequestResponseHandlerController} that can be used to obtain an
   *                                         instance of {@link AsyncRequestResponseHandler}.
   * @throws RestServiceException if the channel creation tasks failed for any reason.
   */
  public NettyMessageProcessor(NettyMetrics nettyMetrics, NettyConfig nettyConfig,
      RequestResponseHandlerController requestResponseHandlerController)
      throws RestServiceException {
    this.nettyMetrics = nettyMetrics;
    this.nettyConfig = nettyConfig;
    requestHandler = requestResponseHandlerController.getHandler();
    if (requestHandler == null) {
      nettyMetrics.channelCreationTasksError.inc();
      throw new RestServiceException("Request handler received during channel bootstrap was null",
          RestServiceErrorCode.ChannelCreationTasksFailure);
    }
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
    logger.trace("Channel {} active", ctx.channel());
    this.ctx = ctx;
    nettyMetrics.channelCreationRate.mark();
  }

  /**
   * Netty calls this function when channel becomes inactive. The channel becomes inactive AFTER it is closed. Any tasks
   * that are performed at this point in time cannot communicate with the client.
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
      logger.error("Request {} was aborted because the channel became inactive", request.getUri());
      onRequestAborted(new ClosedChannelException());
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
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    try {
      nettyMetrics.processorExceptionCaughtCount.inc();
      if (cause instanceof RestServiceException) {
        RestServiceErrorCode errorCode = ((RestServiceException) cause).getErrorCode();
        if (ResponseStatus.getResponseStatus(errorCode) == ResponseStatus.BadRequest) {
          logger.debug("Error on channel {}", ctx.channel(), errorCode, cause);
        } else {
          logger.error("Error on channel {}", ctx.channel(), errorCode, cause);
        }
      } else {
        logger.error("Error on channel {}", ctx.channel(), cause);
      }
      onRequestAborted(cause);
    } catch (Exception e) {
      String uri = (request != null) ? request.getUri() : null;
      nettyMetrics.exceptionCaughtTasksError.inc();
      logger.error("Swallowing exception during exceptionCaught tasks on channel {} for request {}", ctx.channel(), uri,
          e);
      ctx.close();
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
    // This ensures that multiple chunk requests that a handler may be tracking is cleaned up properly. We need this
    // especially because request handlers handle multiple requests at the same time and might have some state for each
    // connection.
    if (event instanceof IdleStateEvent && ((IdleStateEvent) event).state() == IdleState.ALL_IDLE) {
      logger.info("Channel {} has been idle for {} seconds. Closing it", ctx.channel(),
          nettyConfig.nettyServerIdleTimeSeconds);
      nettyMetrics.idleConnectionCloseCount.inc();
      if (request != null) {
        onRequestAborted(new ClosedChannelException());
      }
    }
  }

  /**
   * Netty calls this function whenever data is available on the channel that can be read.
   * <p/>
   * {@link HttpRequest} is converted to {@link NettyRequest} and passed to the the {@link AsyncRequestResponseHandler}.
   * <p/>
   * {@link HttpContent} is added to the {@link NettyRequest} currently being served by the channel.
   * @param ctx The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param obj The {@link HttpObject} that forms a part of a request.
   * @throws RestServiceException if there is an error handling the processing of the current {@link HttpObject}.
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws RestServiceException {
    logger.trace("Reading on channel {}", ctx.channel());
    long currentTime = System.currentTimeMillis();
    if (lastChannelReadTime != null) {
      nettyMetrics.channelReadIntervalInMs.update(currentTime - lastChannelReadTime);
      logger.trace("Delay between channel reads is {} ms", (currentTime - lastChannelReadTime));
    }
    lastChannelReadTime = currentTime;

    nettyMetrics.httpObjectArrivalRate.mark();
    if (request == null) {
      responseChannel = new NettyResponseChannel(ctx, nettyMetrics);
      logger.trace("Created RestResponseChannel for channel {}", ctx.channel());
    }
    if (obj instanceof HttpRequest) {
      if (obj.getDecoderResult().isSuccess()) {
        handleRequest((HttpRequest) obj);
      } else {
        logger.warn("Decoder failed because of malformed request on channel {}", ctx.channel());
        nettyMetrics.malformedRequestError.inc();
        throw new RestServiceException("Decoder failed because of malformed request",
            RestServiceErrorCode.MalformedRequest);
      }
    } else if (obj instanceof HttpContent) {
      handleContent((HttpContent) obj);
    } else {
      logger.warn("Received null/unrecognized HttpObject {} on channel {}", obj, ctx.channel());
      nettyMetrics.unknownHttpObjectError.inc();
      throw new RestServiceException("HttpObject received is null or not of a known type",
          RestServiceErrorCode.UnknownHttpObject);
    }
  }

  /**
   * Handles a {@link HttpRequest}.
   * <p/>
   * Does some state maintenance for all HTTP methods by creating a {@link RestRequest} wrapping this
   * {@link HttpRequest}
   * <p/>
   * In case of POST, delegates handling of {@link RestRequest} to the {@link AsyncRequestResponseHandler}.
   * @param httpRequest the {@link HttpRequest} that needs to be handled.
   * @throws RestServiceException if there is an error handling the current {@link HttpRequest}.
   */
  private void handleRequest(HttpRequest httpRequest)
      throws RestServiceException {
    // We need to maintain state about the request itself for the subsequent parts (if any) that come in. We will attach
    // content to the request as the content arrives.
    if (request == null) {
      long processingStartTime = System.currentTimeMillis();
      try {
        nettyMetrics.requestArrivalRate.mark();
        request = new NettyRequest(httpRequest, nettyMetrics);
        responseChannel.setRequest(request);
        logger.trace("Channel {} now handling request {}", ctx.channel(), request.getUri());
      } finally {
        request.getMetricsTracker().nioMetricsTracker
            .addToRequestProcessingTime(System.currentTimeMillis() - processingStartTime);
      }
      // We send POST for handling immediately since we expect valid content with it.
      // With any other method that we support, we do not expect any valid content. LastHttpContent is a Netty thing.
      // So we wait for LastHttpContent (throw an error if we don't receive it or receive something else) and then
      // schedule the other methods for handling in handleContent().
      if (request.getRestMethod().equals(RestMethod.POST)) {
        requestHandler.handleRequest(request, responseChannel);
      }
    } else {
      // We have received a request when we were not expecting one. This shouldn't happen and there is no good way to
      // deal with it. So just update a metric and log an error.
      logger.warn("Discarding unexpected request on channel {}. Request under processing: {}. Unexpected request: {}",
          ctx.channel(), request.getUri(), httpRequest.getUri());
      nettyMetrics.duplicateRequestError.inc();
    }
  }

  /**
   * Handles a {@link HttpContent}.
   * <p/>
   * Checks to see that a valid {@link RestRequest} is available so that the content can be pushed into the request.
   * <p/>
   * If the HTTP method for the request is something other than POST, delegates handling of {@link RestRequest} to the
   * {@link AsyncRequestResponseHandler} when {@link LastHttpContent} is received.
   * @param httpContent the {@link HttpContent} that needs to be handled.
   * @throws RestServiceException if there is an error handling the current {@link HttpContent}.
   */
  private void handleContent(HttpContent httpContent)
      throws RestServiceException {
    if (request != null) {
      long processingStartTime = System.currentTimeMillis();
      nettyMetrics.bytesReadRate.mark(httpContent.content().readableBytes());
      try {
        logger.trace("Received content for request - {}", request.getUri());
        try {
          request.addContent(httpContent);
        } catch (IllegalStateException e) {
          nettyMetrics.contentAdditionError.inc();
          throw new RestServiceException(e, RestServiceErrorCode.InvalidRequestState);
        } catch (ClosedChannelException e) {
          nettyMetrics.contentAdditionError.inc();
          throw new RestServiceException("The request has been closed and is not accepting content",
              RestServiceErrorCode.RequestChannelClosed);
        }
      } finally {
        long chunkProcessingTime = System.currentTimeMillis() - processingStartTime;
        nettyMetrics.requestChunkProcessingTimeInMs.update(chunkProcessingTime);
        request.getMetricsTracker().nioMetricsTracker.addToRequestProcessingTime(chunkProcessingTime);
      }
      if (!request.getRestMethod().equals(RestMethod.POST)) {
        requestHandler.handleRequest(request, responseChannel);
      }
    } else {
      logger.warn("Received content without a request on channel {}", ctx.channel());
      nettyMetrics.noRequestError.inc();
      throw new RestServiceException("Received content without a request", RestServiceErrorCode.InvalidRequestState);
    }
  }

  /**
   * Performs tasks that need to be performed when the request is aborted.
   * @param cause the reason the request was aborted.
   */
  private void onRequestAborted(Throwable cause) {
    if (responseChannel != null) {
      if (request != null) {
        logger.trace("Request {} is aborted", request.getUri());
      }
      responseChannel.onResponseComplete(cause);
    } else {
      // we specifically do not send an error response to the client directly (through ctx) at this point because of
      // the state transitions that we expect. If the client has sent *anything* at all to us after the completion of
      // the last request (or just after channel became active if this is a newly created channel), we would have a
      // response channel. If we do not have a response channel, it is because the client hasn't sent us anything and
      // isn't *expecting* a response.
      // However we close the channel so that the client realizes something is wrong.
      logger.warn("No RestResponseChannel available for channel {}. Could not send error to client. Closing channel",
          ctx.channel());
      nettyMetrics.missingResponseChannelError.inc();
      ctx.close();
    }
  }
}
