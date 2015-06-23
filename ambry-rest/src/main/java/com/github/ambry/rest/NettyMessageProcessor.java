package com.github.ambry.rest;

import com.github.ambry.restservice.RestRequestHandler;
import com.github.ambry.restservice.RestRequestHandlerController;
import com.github.ambry.restservice.RestRequestInfo;
import com.github.ambry.restservice.RestRequestMetadata;
import com.github.ambry.restservice.RestResponseHandler;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
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
 * ({@link RestRequestMetadata}, {@link com.github.ambry.restservice.RestRequestContent}) and passes it down the
 * pipeline  to a {@link com.github.ambry.restservice.BlobStorageService} through a {@link RestRequestHandler}.
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
 * reply to the client - Similar to the {@link RestRequestHandler}, a {@link RestResponseHandler} needs to maintained
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
  private final RestRequestHandlerController requestHandlerController;
  private final NettyMetrics nettyMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // Each of these live through the lifetime of a request.
  // TODO: when we support keepalive, we need to clear some of these and repopulate them once a request is finished.
  private ChannelHandlerContext ctx = null;
  private RestRequestMetadata request = null;
  private RestRequestHandler requestHandler = null;
  private RestResponseHandler responseHandler = null;

  public NettyMessageProcessor(RestRequestHandlerController requestHandlerController, NettyMetrics nettyMetrics) {
    this.requestHandlerController = requestHandlerController;
    this.nettyMetrics = nettyMetrics;
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
    this.ctx = ctx;
    // As soon as the channel is active, we create an instance of NettyResponseHandler to use for this request.
    // We also get an instance of AsyncRequestHandler from the RequestHandlerController to use for this request. Since
    // the request parts have to be processed in order, we maintain references to the handlers throughout and use the
    // same handlers for the whole request.
    requestHandler = requestHandlerController.getRequestHandler();
    responseHandler = new NettyResponseHandler(ctx, nettyMetrics);
    if (requestHandler == null || responseHandler == null) {
      nettyMetrics.channelActiveTasksFailureCount.inc();
      String msg =
          requestHandler == null ? "RestRequestHandler received was null" : "RestResponseHandler received was null";
      logger.error(msg);
      throw new RestServiceException(msg, RestServiceErrorCode.ChannelActiveTasksFailure);
    }
  }

  /**
   * Netty calls this function when channel becomes inactive. The channel becomes inactive AFTER it is closed. Any tasks
   * at this point of time cannot communicate with the client.
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
    // TODO: once we support keepalive, we need to do this at the end of a request too.
    try {
      if (requestHandler != null) {
        requestHandler.onRequestComplete(request);
      }

      if (responseHandler != null) {
        responseHandler.onRequestComplete(request);
      }
    } catch (Exception e) {
      logger.error("Unable to perform cleanup tasks. Swallowing exception..", e);
      nettyMetrics.channelInactiveTasksFailureCount.inc();
    }
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
    try {
      if (responseHandler != null) {
        responseHandler.onError(request, cause);
      } else {
        //TODO: metric
        logger.error("No response handler found while trying to relay error message. Reporting "
            + HttpResponseStatus.INTERNAL_SERVER_ERROR);
        sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (Exception e) {
      //TODO: metric
      logger.error("Caught exception while trying to handle an error", e);
      sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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
    // Even in that situation, any cleanup code that we have in the handlers will have to be called (when
    // channelInactive() is called as a result of the close). This ensures that multiple chunk requests that a handler
    // may be tracking is cleaned up properly. We need this especially because request handlers handle multiple requests
    // at the same time and may evolve to have some sort of state for each connection.

    // TODO: This needs a unit test - I do not have any idea how to test it currently
    if (event instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) event;
      if (e.state() == IdleState.ALL_IDLE) {
        logger.error("Connection timed out. Closing channel");
        // close() triggers channelInactive() that will trigger onRequestComplete() of the request and response handlers
        if (responseHandler != null) {
          responseHandler.close();
        } else {
          ctx.close();
        }
      } else {
        logger.error("Unrecognized idle state event - " + e.state());
      }
    } else {
      logger.error("Unrecognized user event - " + event);
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
    logger.trace("Reading on channel " + ctx.channel() + " from " + ctx.channel().remoteAddress());
    if (obj != null && obj instanceof HttpRequest) {
      if (obj.getDecoderResult().isSuccess()) {
        handleRequest((HttpRequest) obj);
      } else {
        logger.error("Malformed request received - " + obj);
        nettyMetrics.malformedRequestErrorCount.inc();
        throw new RestServiceException("Malformed request received - " + obj, RestServiceErrorCode.MalformedRequest);
      }
    } else if (obj != null && obj instanceof HttpContent) {
      handleContent((HttpContent) obj);
    } else {
      nettyMetrics.unknownHttpObjectErrorCount.inc();
      throw new RestServiceException("Content received is null or not of a known type",
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
    RestRequestMetadata restRequestMetadata = new NettyRequestMetadata(httpRequest);
    if (request == null) {
      request = restRequestMetadata;
      handleRequestInfo(new RestRequestInfo(request, null, responseHandler));
    } else {
      // We have received a duplicate request. This shouldn't happen and there is no good way to deal with it. So
      // just update a metric and log an error.
      nettyMetrics.duplicateRequestErrorCount.inc();
      logger.error(
          "Received duplicate request. Old request - " + request + ". New request - " + httpRequest + " on channel "
              + ctx.channel());
    }
  }

  /**
   * Handles a {@link HttpContent}.
   * <p/>
   * Checks to see that a valid {@link RestRequestMetadata} is available for bundling with this part of the request and
   * passes a {@link RestRequestInfo} containing a {@link com.github.ambry.restservice.RestRequestContent} wrapping this
   * {@link HttpContent} and an instance of {@link RestResponseHandler} to the {@link RestRequestHandler}.
   * @param httpContent - the {@link HttpContent} that needs to be handled.
   * @throws RestServiceException
   */
  private void handleContent(HttpContent httpContent)
      throws RestServiceException {
    if (request != null) {
      handleRequestInfo(new RestRequestInfo(request, new NettyRequestContent(httpContent), responseHandler));
    } else {
      nettyMetrics.noRequestErrorCount.inc();
      throw new RestServiceException("Received data without a request", RestServiceErrorCode.NoRequest);
    }
  }

  /**
   * Passes the {@link RestRequestInfo} onto a {@link RestRequestHandler}.
   * <p/>
   * When the {@link RestRequestHandler#handleRequest(RestRequestInfo)} returns, there is no guarantee that the
   * request has been handled.
   * @param restRequestInfo
   * @throws RestServiceException
   */
  private void handleRequestInfo(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    try {
      requestHandler.handleRequest(restRequestInfo);
    } catch (RestServiceException e) {
      recordHandlingError(e);
      throw e;
    } catch (Exception e) {
      recordHandlingError(e);
      throw new RestServiceException("Request handling error - ", e, RestServiceErrorCode.RequestHandleFailure);
    }
  }

  /**
   * Logs errors and tracks metrics.
   * @param e - the Exception that occurred.
   */
  private void recordHandlingError(Exception e) {
    logger.error("Handling error for request - " + request.getUri(), e);
    nettyMetrics.handleRequestFailureCount.inc();
  }

  /**
   * For errors that occur before we have a {@link RestResponseHandler} ({@link NettyResponseHandler}) ready.
   * @param status - the response status
   */
  private void sendError(HttpResponseStatus status) {
    String msg = "Failure: " + status + "\r\n";
    FullHttpResponse response =
        new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

    if (ctx.channel().isActive()) {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
