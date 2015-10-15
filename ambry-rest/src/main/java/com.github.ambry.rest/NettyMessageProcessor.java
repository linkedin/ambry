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
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import java.nio.channels.ClosedChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * Inbound request handler for Netty.
 * <p/>
 * It processes a request (in parts) by converting it from Netty specific objects ({@link HttpObject},
 * {@link HttpRequest}, {@link HttpContent}) into {@link RestRequest}, a generic request object that all the RESTful
 * layers can understand and passes it down the pipeline to a {@link BlobStorageService} through a
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
class NettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  private final NettyMetrics nettyMetrics;
  private final NettyConfig nettyConfig;
  private final RestRequestHandler requestHandler;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // variables that will live through the life of the channel.
  private ChannelHandlerContext ctx = null;

  // variables that will live for the life of a single request.
  private volatile NettyRequest request = null;
  private volatile NettyResponseChannel responseChannel = null;

  public NettyMessageProcessor(NettyMetrics nettyMetrics, NettyConfig nettyConfig,
      RestRequestHandlerController requestHandlerController)
      throws RestServiceException {
    this.nettyMetrics = nettyMetrics;
    this.nettyConfig = nettyConfig;
    requestHandler = requestHandlerController.getRequestHandler();
    if (requestHandler == null) {
      nettyMetrics.channelActiveTasksError.inc();
      throw new RestServiceException("RestRequestHandler received during channel bootstrap was null",
          RestServiceErrorCode.ChannelCreationTasksFailure);
    }
    logger.trace("Instantiated NettyMessageProcessor");
    nettyMetrics.processorCreationRate.mark();
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
    if (request != null) {
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
      HttpResponseStatus backupResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      String backupErrorMessage = null;
      if (cause instanceof RestServiceException) {
        RestServiceErrorCode errorCode = ((RestServiceException) cause).getErrorCode();
        if (ResponseStatus.getResponseStatus(errorCode) == ResponseStatus.BadRequest) {
          backupResponseStatus = HttpResponseStatus.BAD_REQUEST;
          backupErrorMessage = cause.getMessage();
          logger.debug("Error on channel {} with error code {}", ctx.channel(), errorCode, cause);
        } else {
          logger.error("Error on channel {} with error code {}", ctx.channel(), errorCode, cause);
        }
      } else {
        logger.error("Error on channel {}", ctx.channel(), cause);
      }
      nettyMetrics.processorExceptionCaught.inc();
      if (responseChannel == null) {
        logger.warn("No RestResponseChannel available for channel {}. Sending error response to client directly",
            ctx.channel());
        nettyMetrics.missingResponseChannelError.inc();
        sendError(backupResponseStatus, backupErrorMessage);
      }
      onRequestAborted(cause);
    } catch (Exception e) {
      String uri = (request != null) ? request.getUri() : null;
      logger.error("Swallowing exception during exceptionCaught tasks on channel {} for request {}", ctx.channel(), uri,
          e);
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
      nettyMetrics.idleConnectionClose.inc();
      if (responseChannel == null) {
        ctx.close();
      }
      onRequestAborted(new ClosedChannelException());
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
   * Does some state maintenance for all HTTP methods by creating a {@link RestRequest} wrapping this
   * {@link HttpRequest}
   * <p/>
   * In case of POST, delegates handling of {@link RestRequest} to the {@link RestRequestHandler}.
   * @param httpRequest the {@link HttpRequest} that needs to be handled.
   * @throws RestServiceException if there is an error handling the current {@link HttpRequest}.
   */
  private void handleRequest(HttpRequest httpRequest)
      throws RestServiceException {
    // We need to maintain state about the request itself for the subsequent parts (if any) that come in. We will attach
    // content to the request as the content arrives.
    if (request == null) {
      nettyMetrics.requestArrivalRate.mark();
      request = new NettyRequest(httpRequest);
      responseChannel = new NettyResponseChannel(ctx, nettyMetrics);
      logger.trace("Channel {} now handling request {}", ctx.channel(), request.getUri());
      // We send POST for handling immediately since we expect valid content with it.
      // With any other method that we support, we do not expect any valid content. LastHttpContent is a Netty thing.
      // So we wait for LastHttpContent (throw an error if we don't receive it or receive something else) and then
      // schedule the other methods for handling in handleContent().
      if (RestMethod.POST.equals(request.getRestMethod())) {
        requestHandler.handleRequest(request, responseChannel);
      }
    } else {
      // We have received a duplicate request. This shouldn't happen and there is no good way to deal with it. So
      // just update a metric and log an error.
      // TODO: rethink this during keep-alive - this probably calls for a close.
      logger.warn("Discarding duplicate request on channel {}. Current request - {}. Duplicate request - {}",
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
   * {@link RestRequestHandler} when {@link LastHttpContent} is received.
   * @param httpContent the {@link HttpContent} that needs to be handled.
   * @throws RestServiceException if there is an error handling the current {@link HttpContent}.
   */
  private void handleContent(HttpContent httpContent)
      throws RestServiceException {
    try {
      if (request != null) {
        logger.trace("Received content for request - {}", request.getUri());
        request.addContent(httpContent);
        if (!RestMethod.POST.equals(request.getRestMethod())) {
          requestHandler.handleRequest(request, responseChannel);
        }
      } else {
        logger.warn("Received content without a request on channel {}", ctx.channel());
        nettyMetrics.noRequestError.inc();
        throw new RestServiceException("Received content without a request", RestServiceErrorCode.NoRequest);
      }
    } catch (ClosedChannelException e) {
      throw new RestServiceException("The request has been closed and is not accepting content",
          RestServiceErrorCode.RequestChannelClosed);
    }
  }

  /**
   * Performs tasks that need to be performed when the request is aborted.
   * @param cause the reason the request was aborted.
   */
  private void onRequestAborted(Throwable cause) {
    if (request != null) {
      logger.trace("Request {} is aborted", request.getUri());
      if (responseChannel != null) {
        responseChannel.onResponseComplete(cause);
      }
    }
  }

  /**
   * For errors that occur before we have a {@link RestResponseChannel} ({@link NettyResponseChannel}) ready.
   * @param status the response status code
   * @param errorMsg optional error message
   */
  private void sendError(HttpResponseStatus status, String errorMsg) {
    StringBuilder msg = new StringBuilder("Failure: ").append(status);
    if (errorMsg != null) {
      msg.append(errorMsg);
    }
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
