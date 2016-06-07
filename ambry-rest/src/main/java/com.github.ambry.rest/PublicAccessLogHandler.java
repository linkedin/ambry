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
 * Captures headers and other required info from request and responses, to make public access log entries
 * {@link PublicAccessLogger} assists in logging the required information
 */
public class PublicAccessLogHandler extends ChannelDuplexHandler {
  private final PublicAccessLogger publicAccessLogger;
  private final NettyMetrics nettyMetrics;
  private long requestArrivalTimeInMs;
  private long requestLastChunkArrivalTimeInMs;
  private long responseFirstChunkStartTimeInMs;
  private StringBuilder logMessage;
  private HttpRequest request;

  private static final long INIT_TIME = -1;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public PublicAccessLogHandler(PublicAccessLogger publicAccessLogger, NettyMetrics nettyMetrics) {
    this.publicAccessLogger = publicAccessLogger;
    this.nettyMetrics = nettyMetrics;
    reset();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj)
      throws Exception {
    logger.trace("Reading on channel {}", ctx.channel());
    long startTimeInMs = System.currentTimeMillis();
    if (obj instanceof HttpRequest) {
      nettyMetrics.publicAccessLogRequestRate.mark();
      if (request != null) {
        logDurations();
        logMessage.append(" : Received request while another request in progress. Resetting log message.");
        logger.error(logMessage.toString());
      }
      reset();
      requestArrivalTimeInMs = System.currentTimeMillis();
      request = (HttpRequest) obj;
      logMessage.append(ctx.channel().remoteAddress()).append(" ");
      logMessage.append(request.getMethod().toString()).append(" ");
      logMessage.append(request.getUri()).append(", ");
      logHeaders("Request", request, publicAccessLogger.getRequestHeaders());
      logMessage.append(", ");
    } else if (obj instanceof LastHttpContent) {
      requestLastChunkArrivalTimeInMs = System.currentTimeMillis();
    } else if (!(obj instanceof HttpContent)) {
      logger.error("Receiving request (messageReceived) that is not of type HttpRequest or HttpContent. " +
          "Receiving request from " + ctx.channel().remoteAddress() + ". " +
          "Request is of type " + obj.getClass() + ". " +
          "No action being taken other than logging this unexpected state.");
    }
    nettyMetrics.publicAccessLogRequestProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
    super.channelRead(ctx, obj);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    long startTimeInMs = System.currentTimeMillis();
    boolean shouldReset = msg instanceof LastHttpContent;
    if (request != null) {
      if (msg instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) msg;
        logHeaders("Response", response, publicAccessLogger.getResponseHeaders());
        logMessage.append(", ");
        logMessage.append("status=").append(response.getStatus().code());
        logMessage.append(", ");
        if (HttpHeaders.isTransferEncodingChunked(response)) {
          responseFirstChunkStartTimeInMs = System.currentTimeMillis();
        } else {
          shouldReset = true;
        }
      } else if (!(msg instanceof HttpContent)) {
        logger.error(
            "Sending response that is not of type HttpResponse or HttpContent. Sending response to " + ctx.channel()
                .remoteAddress() + ". Request is of type " + msg.getClass()
                + ". No action being taken other than logging this unexpected state.");
      }
      if (shouldReset) {
        logDurations();
        publicAccessLogger.logInfo(logMessage.toString());
        reset();
      }
    }
    nettyMetrics.publicAccessLogResponseProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
    super.write(ctx, msg, promise);
  }

  @Override
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise future)
      throws Exception {
    if (request != null) {
      logError(" : Channel disconnected while request in progress.");
    }
    super.disconnect(ctx, future);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise future)
      throws Exception {
    if (request != null) {
      logError(" : Channel closed while request in progress.");
    }
    super.close(ctx, future);
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

  /**
   * Resets some variables as part of logging a response
   */
  private void reset() {
    responseFirstChunkStartTimeInMs = INIT_TIME;
    requestLastChunkArrivalTimeInMs = INIT_TIME;
    requestArrivalTimeInMs = INIT_TIME;
    logMessage = new StringBuilder();
    request = null;
  }

  /**
   * Logs error message
   * @param msg
   */
  private void logError(String msg) {
    logDurations();
    logMessage.append(msg);
    publicAccessLogger.logError(logMessage.toString());
  }
}
