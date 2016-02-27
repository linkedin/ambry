package com.github.ambry.rest;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Captures headers and other required info from request and responses to make public access log entries
 * {@link PublicAccessLogger} assists in logging the required information
 */
public class PublicAccessLogRequestHandler extends ChannelDuplexHandler {
  private final PublicAccessLogger publicAccessLogger;
  private long requestArrivalTimeInMs;
  private long requestLastChunkArrivalTimeInMs;
  private long responseFirstChunkStartTimeInMs;
  private boolean requestInProgress = false;
  private volatile StringBuilder logMessage;
  private volatile HttpRequest request;

  private static final long INIT_TIME = -1;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public PublicAccessLogRequestHandler(PublicAccessLogger publicAccessLogger) {
    this.publicAccessLogger = publicAccessLogger;
  }

  /**
   * Appends specified headers to the log message if those headers are not null
   * @param tag pretty name for set of headers to append
   * @param message http message from which to log headers
   * @param headers array of headers to log
   */
  private void logHeaders(String tag, HttpMessage message, String[] headers) {
    logMessage.append(tag).append(" (");
    for (String header : headers) {
      if (message.headers().get(header) != null) {
        logMessage.append("[").append(header).append("=").append(message.headers().get(header)).append("] ");
      }
    }
    boolean isChunked = HttpHeaders.isTransferEncodingChunked(message);
    logMessage.append("[isChunked=").append(isChunked).append("]");
    logMessage.append(")");
  }

  /**
   * Appends duration of operation to the log message. Also appends duration of receive or send phases of
   * operations if those phases used chunked encoding.
   */
  private void logDurations() {
    long nowMs = System.currentTimeMillis();
    logMessage.append("duration=").append(nowMs - requestArrivalTimeInMs).append("ms ");
    if (requestLastChunkArrivalTimeInMs != INIT_TIME) {
      logMessage.append("(chunked request receive=").append(requestLastChunkArrivalTimeInMs - requestArrivalTimeInMs)
          .append("ms) ");
    }
    if (responseFirstChunkStartTimeInMs != INIT_TIME) {
      logMessage.append("(chunked response send=").append(nowMs - responseFirstChunkStartTimeInMs).append("ms) ");
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj)
      throws Exception {
    logger.trace("Reading on channel {}", ctx.channel());
    if (obj instanceof HttpRequest) {
      if (requestInProgress) {
        logDurations();
        logMessage.append(" : Received request while another request in progress. Resetting log message.");
        logger.error(logMessage.toString());
      }
      logMessage = new StringBuilder();
      requestArrivalTimeInMs = System.currentTimeMillis();
      reset(true);
      request = (HttpRequest) obj;

      logMessage.append(ctx.channel().remoteAddress()).append(" ");
      logMessage.append(request.getMethod().toString()).append(" ");
      logMessage.append(request.getUri()).append(", ");
      logHeaders("Request", request, publicAccessLogger.getRequestHeaders());
      logMessage.append(", ");
    } else if (obj instanceof HttpContent) {
      HttpContent httpContent = (HttpContent) obj;
      if (httpContent instanceof LastHttpContent) {
        requestLastChunkArrivalTimeInMs = System.currentTimeMillis();
      }
    } else {
      logger.error("Receiving request (messageReceived) that is not of type HttpRequest or HttpChunk. " +
          "Receiving request from " + ctx.channel().remoteAddress() + ". " +
          "Request is of type " + obj.getClass() + ". " +
          "No action being taken other than logging this unexpected state.");
    }
    super.channelRead(ctx, obj);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (requestInProgress) {
      boolean recognized = false;
      if (msg instanceof HttpResponse) {
        recognized = true;
        HttpResponse response = (HttpResponse) msg;
        logHeaders("Response", response, publicAccessLogger.getResponseHeaders());
        logMessage.append(", ");
        logMessage.append("status=").append(response.getStatus().code());
        logMessage.append(", ");
        if (HttpHeaders.isTransferEncodingChunked(response)) {
          responseFirstChunkStartTimeInMs = System.currentTimeMillis();
        }
      }
      if (msg instanceof LastHttpContent) {
        recognized = true;
        logDurations();
        reset(false);
        publicAccessLogger.logInfo(logMessage.toString());
      }
      if (!recognized && !(msg instanceof HttpContent)) {
        logger.error("Sending response (writeRequested) that is not of type Response or LastHttpContent. " +
            "Sending response to " + ctx.channel().remoteAddress() + ". " +
            "Request is of type " + msg.getClass() + ". " +
            "No action being taken other than logging this unexpected state.");
      }
    } else {
      logger.error("Sending response (writeRequested) without any request in progress. " +
          "Sending response to " + ctx.channel().remoteAddress() + ". " +
          "No action being taken other than logging this unexpected state.");
    }
    super.write(ctx, msg, promise);
  }

  /**
   * Resets some variables as part of logging a response
   */
  private void reset(boolean requestInProgress) {
    this.requestInProgress = requestInProgress;
    responseFirstChunkStartTimeInMs = INIT_TIME;
    requestLastChunkArrivalTimeInMs = INIT_TIME;
  }

  @Override
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise future)
      throws Exception {
    if (requestInProgress) {
      logError(" : Channel disconnected while request in progress.");
    }
    super.disconnect(ctx, future);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise future)
      throws Exception {
    if (requestInProgress) {
      logError(" : Channel closed while request in progress.");
    }
    super.close(ctx, future);
  }

  /**
   * Rests the requestInProgress and logs error message
   * @param msg
   */
  private void logError(String msg) {
    requestInProgress = false;
    logDurations();
    logMessage.append(msg);
    publicAccessLogger.logError(logMessage.toString());
  }
}
