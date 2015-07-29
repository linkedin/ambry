package com.github.ambry.rest;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * Inbound request handler for Netty.
 * <p/>
 * It processes a request (in parts) by converting it from Netty specific objects ({@link HttpObject},
 * {@link HttpRequest}, {@link HttpContent}) into generic objects that all the RESTful layers can understand
 * ({@link RestRequestMetadata}, {@link RestRequestContent}) and passes it down the
 * pipeline  to a {@link BlobStorageService} through a {@link RestRequestHandler}.
 * <p/>
 * It is also responsible for maintaining three critical pieces of state: -
 * 1. {@link RestRequestMetadata} of a request - The same reference of {@link RestRequestMetadata} has to be attached
 * to each part of a single request to identify them as belonging to the same request. This is required by the
 * underlying layers.
 * 2. {@link RestRequestHandler} assigned to handle the request - All parts of a single request have to be assigned to
 * the same {@link RestRequestHandler} since they have to be processed in order and might need state maintenance at the
 * lower layers. Therefore a {@link RestRequestHandler} is obtained at the beginning of the request, used for all parts
 * of the request and discarded only when the request is complete.
 * 3. {@link RestResponseHandler} that needs to be bundled with all request parts so that the underlying layers can
 * reply to the client - Similar to the {@link RestRequestHandler}, a {@link RestResponseHandler} needs to be maintained
 * for the lifetime of a request. This should be a Netty specific implementation ({@link NettyResponseHandler}).
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
  private final RestRequestHandlerController requestHandlerController;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private ChannelHandlerContext ctx = null;
  private RestRequestMetadata request = null;
  private RestRequestHandler requestHandler = null;
  private RestResponseHandler responseHandler = null;

  public NettyMessageProcessor(NettyMetrics nettyMetrics, NettyConfig nettyConfig,
      RestRequestHandlerController requestHandlerController) {
    this.nettyMetrics = nettyMetrics;
    this.nettyConfig = nettyConfig;
    this.requestHandlerController = requestHandlerController;
    logger.trace("Instantiated NettyMessageProcessor");
    nettyMetrics.processorCreationRate.mark();
  }

  /**
   * Netty calls this function when the channel is active.
   * <p/>
   * This is called exactly once in the lifetime of the channel. If there is no keepalive, this will be called
   * before the request (and the channel lives to serve exactly one request). If there is keepalive, this will be
   * called just once before receiving the first request.
   * <p/>
   * At this point the required {@link RestRequestHandler} and {@link RestResponseHandler} are obtained and the same
   * instances are used throughout the lifetime of the channel.
   * <p/>
   * While a new instance of {@link NettyResponseHandler} is created, an instance of {@link RestRequestHandler} is
   * requested from the {@link RestRequestHandlerController}.
   * @param ctx  - The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @throws RestServiceException
   */
  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws RestServiceException {
    logger.trace("Channel {} active", ctx.channel());
    this.ctx = ctx;
    // As soon as the channel is active, we create an instance of NettyResponseHandler to use for this request.
    // We also get an instance of AsyncRequestHandler from the RequestHandlerController to use for this request. Since
    // the request parts have to be processed in order, we maintain references to the handlers throughout and use the
    // same handlers for the whole request.
    requestHandler = requestHandlerController.getRequestHandler();
    responseHandler = new NettyResponseHandler(ctx, nettyMetrics);
    if (requestHandler == null || responseHandler == null) {
      nettyMetrics.channelActiveTasksError.inc();
      String msg = (requestHandler == null) ? "RestRequestHandler received during channel bootstrap was null"
          : "RestResponseHandler received during channel bootstrap was null";
      throw new RestServiceException(msg, RestServiceErrorCode.ChannelActiveTasksFailure);
    }
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
   * @param ctx - The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    logger.trace("Channel {} inactive", ctx.channel());
    nettyMetrics.channelDestructionRate.mark();
    onRequestComplete(null, true);
  }

  /**
   * Netty calls this function when any exception is caught during the functioning of this handler.
   * <p/>
   * Centralized error handling based on the exception is performed here. Error responses are sent to the client via
   * the {@link RestResponseHandler} wherever possible.
   * <p/>
   * If this function throws an Exception, it is bubbled up to the handler before this one in the Netty pipeline.
   * @param ctx - The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param cause - The cause of the error.
   * @throws Exception
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    if (cause instanceof RestServiceException) {
      RestServiceErrorCode errorCode = ((RestServiceException) cause).getErrorCode();
      if (RestServiceErrorCode.getErrorCodeGroup(errorCode) == RestServiceErrorCode.BadRequest) {
        logger.debug("Error on channel {} with error code {}", ctx.channel(), errorCode, cause);
      } else {
        logger.error("Error on channel {} with error code {}", ctx.channel(), errorCode, cause);
      }
    } else {
      logger.error("Error on channel {}", ctx.channel(), cause);
    }
    nettyMetrics.processorExceptionCaught.inc();
    if (responseHandler == null) {
      logger.warn("No RestResponseHandler available for channel {}. Sending error response to client directly",
          ctx.channel());
      nettyMetrics.missingResponseHandlerError.inc();
      sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
    onRequestComplete(cause, false);
  }

  /**
   * Netty calls this function when events that we have registered for, occur (in this case we are specifically waiting
   * for {@link IdleStateEvent} so that we close connections that have been idle too long - maybe due to client failure)
   * @param ctx - The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param event - The event that occurred.
   * @throws Exception
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event)
      throws Exception {
    // NOTE: This is specifically in place to handle connections that close unexpectedly from the client side.
    // Even in that situation, any cleanup code that we have in the handlers will have to be called.
    // This ensures that multiple chunk requests that a handler may be tracking is cleaned up properly. We need this
    // especially because request handlers handle multiple requests at the same time and might have some state for each
    // connection.
    if (event instanceof IdleStateEvent && ((IdleStateEvent) event).state() == IdleState.ALL_IDLE) {
      logger.info("Channel {} has been idle for {} seconds. Closing it", ctx.channel(),
          nettyConfig.nettyServerIdleTimeSeconds);
      nettyMetrics.idleConnectionClose.inc();
      if (responseHandler == null) {
        ctx.close();
      }
      onRequestComplete(null, true);
    }
  }

  /**
   * Netty calls this function whenever data is available on the channel that can be read.
   * <p/>
   * Netty specific objects are converted to generic objects that all RESTful layers can understand and passed to a
   * {@link RestRequestHandler} for handling.
   * @param ctx - The {@link ChannelHandlerContext} that can be used to perform operations on the channel.
   * @param obj - The {@link HttpObject} that forms a part of a request.
   * @throws RestServiceException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws RestServiceException {
    logger.trace("Reading on channel {}", ctx.channel());
    nettyMetrics.httpObjectArrivalRate.mark();
    if (obj != null && obj instanceof HttpRequest) {
      if (obj.getDecoderResult().isSuccess()) {
        handleRequest((HttpRequest) obj);
      } else {
        logger.warn("Decoder failed because of malformed request on channel {}", ctx.channel());
        nettyMetrics.malformedRequestError.inc();
        throw new RestServiceException("Decoder failed because of malformed request",
            RestServiceErrorCode.MalformedRequest);
      }
    } else if (obj != null && obj instanceof HttpContent) {
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
   * Does some state maintenance before passing a {@link RestRequestInfo} containing the {@link RestRequestMetadata}
   * wrapping this {@link HttpRequest} and an instance of {@link RestResponseHandler} to the {@link RestRequestHandler}.
   * @param httpRequest - the {@link HttpRequest} that needs to be handled.
   * @throws RestServiceException
   */
  private void handleRequest(HttpRequest httpRequest)
      throws RestServiceException {
    // We need to maintain state about the request itself for the subsequent parts (if any) that come in. We will
    // attach the same instance of RestRequestMetadata to each part of the same request.
    if (request == null) {
      nettyMetrics.requestArrivalRate.mark();
      request = new NettyRequestMetadata(httpRequest);
      logger.trace("Channel {} now handling request {}", ctx.channel(), request.getUri());
      requestHandler.handleRequest(new RestRequestInfo(request, null, responseHandler, true));
    } else {
      // We have received a duplicate request. This shouldn't happen and there is no good way to deal with it. So
      // just update a metric and log an error.
      logger.warn("Discarding duplicate request on channel {}. Current request - {}. Duplicate request - {}",
          ctx.channel(), request.getUri(), httpRequest.getUri());
      nettyMetrics.duplicateRequestError.inc();
    }
  }

  /**
   * Handles a {@link HttpContent}.
   * <p/>
   * Checks to see that a valid {@link RestRequestMetadata} is available for bundling with this part of the request and
   * passes a {@link RestRequestInfo} containing a {@link RestRequestContent} wrapping this
   * {@link HttpContent} and an instance of {@link RestResponseHandler} to the {@link RestRequestHandler}.
   * @param httpContent - the {@link HttpContent} that needs to be handled.
   * @throws RestServiceException
   */
  private void handleContent(HttpContent httpContent)
      throws RestServiceException {
    if (request != null) {
      logger.trace("Received content for request - {}", request.getUri());
      requestHandler.handleRequest(new RestRequestInfo(request, new NettyRequestContent(httpContent), responseHandler));
    } else {
      logger.warn("Received content without a request on channel {}", ctx.channel());
      nettyMetrics.noRequestError.inc();
      throw new RestServiceException("Received content without a request", RestServiceErrorCode.NoRequest);
    }
  }

  /**
   * Performs tasks that need to be performed when the request is complete. If the request failed, the cause of
   * failure should be forwarded.
   * @param cause - the cause of failure if handling failed, null otherwise.
   * @param forceClose - whether the connection needs to be forcibly closed.
   */
  private void onRequestComplete(Throwable cause, boolean forceClose) {
    String uri = (request != null) ? request.getUri() : null;
    try {
      logger.trace("Request {} is complete", uri);
      if (responseHandler != null) {
        responseHandler.onRequestComplete(cause, forceClose);
      }
      if (requestHandler != null) {
        requestHandler.onRequestComplete(request);
      }
    } catch (Exception e) {
      logger.error("Swallowing exception during onRequestComplete tasks on channel {} for request {} ", ctx.channel(),
          uri, e);
      nettyMetrics.processorRequestCompleteTasksError.inc();
    }
  }

  /**
   * For errors that occur before we have a {@link RestResponseHandler} ({@link NettyResponseHandler}) ready.
   * @param status - the response status
   */
  private void sendError(HttpResponseStatus status) {
    String msg = "Failure: " + status;
    FullHttpResponse response =
        new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
    if (ctx.channel().isActive()) {
      logger.trace("Sending error response {} to the client", msg);
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } else {
      logger.error("Could not send error to client on channel {} because channel is inactive", ctx.channel());
      nettyMetrics.fallbackErrorSendingError.inc();
    }
  }
}
