package com.github.ambry.rest;

import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.RestObject;
import com.github.ambry.restservice.RestRequest;
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
 * Inbound message handler for netty. Responsible for processing the message and maintaining state
 * Also responsible for allocating the right response handler and calling into the right message handler.
 */
public class NettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  private final RestRequestDelegator requestDelegator;
  private final NettyMetrics nettyMetrics;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Each of these live through the lifetime of a request.
   *
   * TODO: when we support keepalive, we need to clear some of these and repopulate them once a request is finished.
   */
  private ChannelHandlerContext ctx = null;
  private RestRequest request = null;
  private RestMessageHandler messageHandler = null;
  private RestResponseHandler responseHandler = null;

  public NettyMessageProcessor(RestRequestDelegator requestDelegator, NettyMetrics nettyMetrics) {
    this.requestDelegator = requestDelegator;
    this.nettyMetrics = nettyMetrics;
  }

  /**
   * Netty calls this function when channel is created. This is called exactly once in the lifetime of the channel
   * @param ctx
   * @throws RestServiceException
   */
  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws RestServiceException {
    this.ctx = ctx;
    try {
      /*
          As soon as the channel is active, we create an instance of NettyResponseHandler to use for this request.
          We also get a RestMessageHandler from the delegator to use for this request. Since the messages have
          to be processed in order, we maintain references to the handlers throughout and use the same handlers
          for the whole request.
       */
      messageHandler = requestDelegator.getMessageHandler();
      responseHandler = new NettyResponseHandler(ctx, nettyMetrics);
    } catch (Exception e) {
      logger.error("Unable to obtain message/response handlers - " + e);
      nettyMetrics.channelActiveTasksFailureCount.inc();
      throw new RestServiceException("Unable to obtain message/response handlers - " + e,
          RestServiceErrorCode.ChannelActiveTasksFailure);
    }
  }

  /**
   * Netty calls this function when channel becomes inactive. This is called exactly once in the lifetime of the channel
   * @param ctx
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    try {
      if (messageHandler != null) {
        messageHandler.onRequestComplete(request);
      }

      if (responseHandler != null) {
        responseHandler.onRequestComplete();
      }
    } catch (Exception e) {
      logger.error("Unable to perform cleanup tasks - " + e + ". Swallowing exception..");
      nettyMetrics.channelInactiveTasksFailureCount.inc();
    }
  }

  /**
   * Netty calls this function when any exception is caught.
   * @param ctx
   * @param cause
   * @throws Exception
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    try {
      if (responseHandler != null) {
        responseHandler.onError(cause);
      } else {
        //TODO: metric
        logger.error("No response handler found while trying to relay error message. Reporting "
            + HttpResponseStatus.INTERNAL_SERVER_ERROR);
        sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (Exception e) {
      //TODO: metric
      logger.error("Caught exception while trying to handle an error - " + e);
      sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Netty calls this function when events that we have registered for occur (in this case we are specifically waiting
   * for idle state events so that we close connections that have been idle too long - maybe due to client failure)
   * @param ctx
   * @param evt
   * @throws Exception
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
      throws Exception {
    /**
     * NOTE: This is specifically in place to handle connections that close unexpectedly from the client side.
     * Even in that situation, any cleanup code that we have in the handlers will have to be called
     * (when channelInactive is called as a result of the close).
     * This ensures that multiple chunk requests that a handler may be tracking is cleaned up properly. We need this
     * especially because request handlers handle multiple requests at the same time and
     * are may evolve to have some sort of state for each connection.
     */

    // TODO: This needs a unit test - I do not have any idea how to test it currently
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.ALL_IDLE) {
        logger.error("Connection timed out. Closing channel");
        ctx.close();
      } else {
        logger.error("Unrecognized idle state event - " + e.state());
      }
    } else {
      logger.error("Unrecognized user event - " + evt);
    }
  }

  /**
   * Netty calls this function whenever data is available on the channel that can be read.
   * @param ctx
   * @param obj
   * @throws RestServiceException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws RestServiceException {
    logger.trace("Reading on channel " + ctx.channel() + " from " + ctx.channel().remoteAddress());
    if (obj != null) {
      if (vetRequest(obj)) {
        RestObject convertedObj = convertObjToGeneric(obj);
        handleMessage(convertedObj);
      } else {
        logger.error("Malformed request received - " + obj);
        nettyMetrics.malformedRequestErrorCount.inc();
        throw new RestServiceException("Malformed request received - " + obj, RestServiceErrorCode.BadRequest);
      }
    }
  }

  /**
   * Deos some state creation, maintenance and hands off the message to the RestMessageHandler.
   * @param obj
   * @throws RestServiceException
   */
  private void handleMessage(RestObject obj)
      throws RestServiceException {
    // We need to maintain state about the request itself for the subsequent chunks (if any) that come in
    if (request == null && obj instanceof RestRequest) {
      request = (RestRequest) obj;
    } else if (request == null) {
      nettyMetrics.noRequestErrorCount.inc();
      throw new RestServiceException("Received data without a request", RestServiceErrorCode.NoRequest);
    } else if (obj instanceof RestRequest) {
      //TODO: should we ignore the duplicate request or return BAD_REQUEST?
      nettyMetrics.duplicateRequestErrorCount.inc();
      throw new RestServiceException("Received duplicate request. Old request - " + request + ". New request - " + obj,
          RestServiceErrorCode.DuplicateRequest);
    }

    try {
      messageHandler.handleMessage(new MessageInfo(request, obj, responseHandler));
    } catch (RestServiceException e) {
      recordHandlingError(e);
      throw e;
    } catch (Exception e) {
      recordHandlingError(e);
      throw new RestServiceException("Message handling error - " + e, RestServiceErrorCode.MessageHandleFailure);
    }
  }

  private void recordHandlingError(Exception e) {
    logger.error("Message handling error for request - " + request.getUri() + " - " + e);
    nettyMetrics.handleRequestFailureCount.inc();
  }

  /**
   * Makes sure we have a good request.
   * @param obj
   * @return
   */
  private Boolean vetRequest(HttpObject obj) {
    if (obj instanceof HttpRequest) {
      HttpRequest httpRequest = (HttpRequest) obj;
      if (!httpRequest.getDecoderResult().isSuccess()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Converts netty objects (HttpRequest, HttpContent) to generic objects that RestMessageHandler and BlobStorageService
   * can understand. (NettyRequest and NettyContent are implementations of these generic objects).
   * @param obj
   * @return
   * @throws RestServiceException
   */
  private RestObject convertObjToGeneric(HttpObject obj)
      throws RestServiceException {
    // convert the object into a something that the other layers will understand.
    try {
      if (obj instanceof HttpRequest) {
        return new NettyRequest((HttpRequest) obj);
      } else if (obj instanceof HttpContent) {
        return new NettyContent((HttpContent) obj);
      } else {
        nettyMetrics.unknownHttpObjectErrorCount.inc();
        throw new Exception("HttpObject received is not of a known type");
      }
    } catch (RestServiceException e) {
      throw e;
    } catch (Exception e) {
      throw new RestServiceException("Http object conversion failed with reason - " + e,
          RestServiceErrorCode.HttpObjectConversionFailure);
    }
  }

  // for errors that occur before we have a RestResponseHandler (NettyResponseHandler) ready.
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
