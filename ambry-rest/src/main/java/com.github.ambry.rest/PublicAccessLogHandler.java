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
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import javax.net.ssl.SSLEngine;
import javax.security.auth.x500.X500Principal;
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
  private StringBuilder sslLogMessage;

  private static final long INIT_TIME = -1;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public PublicAccessLogHandler(PublicAccessLogger publicAccessLogger, NettyMetrics nettyMetrics) {
    this.publicAccessLogger = publicAccessLogger;
    this.nettyMetrics = nettyMetrics;
    reset();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
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
      logMessage.append(request.method().toString()).append(" ");
      logMessage.append(request.uri()).append(", ");
      logSSLInfo(ctx);
      logMessage.append(", ");
      logHeaders("Request", request, publicAccessLogger.getRequestHeaders());
      logMessage.append(", ");
    } else if (obj instanceof LastHttpContent) {
      requestLastChunkArrivalTimeInMs = System.currentTimeMillis();
    } else if (!(obj instanceof HttpContent)) {
      logger.error("Receiving request (messageReceived) that is not of type HttpRequest or HttpContent. "
          + "Receiving request from " + ctx.channel().remoteAddress() + ". " + "Request is of type " + obj.getClass()
          + ". " + "No action being taken other than logging this unexpected state.");
    }
    nettyMetrics.publicAccessLogRequestProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
    super.channelRead(ctx, obj);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    long startTimeInMs = System.currentTimeMillis();
    boolean shouldReset = msg instanceof LastHttpContent;
    if (request != null) {
      if (msg instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) msg;
        logHeaders("Response", response, publicAccessLogger.getResponseHeaders());
        logMessage.append(", ");
        logMessage.append("status=").append(response.status().code());
        logMessage.append(", ");
        if (HttpUtil.isTransferEncodingChunked(response)) {
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
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
    if (request != null) {
      logError(" : Channel disconnected while request in progress.");
    }
    super.disconnect(ctx, future);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
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
    boolean isChunked = HttpUtil.isTransferEncodingChunked(message);
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
      logMessage.append("(chunked request receive=")
          .append(requestLastChunkArrivalTimeInMs - requestArrivalTimeInMs)
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
   * @param msg the message to log
   */
  private void logError(String msg) {
    logDurations();
    logMessage.append(msg);
    publicAccessLogger.logError(logMessage.toString());
  }

  /**
   * If this is an SSL channel, log information about the peer certificate.
   * @param ctx the {@link ChannelHandlerContext} for this channel.
   */
  private void logSSLInfo(ChannelHandlerContext ctx) {
    if (sslLogMessage == null) {
      sslLogMessage = new StringBuilder();
      sslLogMessage.append("SSL (");
      try {
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        boolean sslUsed = sslHandler != null;
        sslLogMessage.append("[used=").append(sslUsed).append("]");
        if (sslUsed) {
          SSLEngine sslEngine = sslHandler.engine();
          if (sslEngine.getNeedClientAuth()) {
            for (Certificate certificate : sslEngine.getSession().getPeerCertificates()) {
              if (certificate instanceof X509Certificate) {
                X500Principal principal = ((X509Certificate) certificate).getSubjectX500Principal();
                Collection subjectAlternativeNames = ((X509Certificate) certificate).getSubjectAlternativeNames();
                sslLogMessage.append(", [principal=").append(principal).append("]");
                sslLogMessage.append(", [san=").append(subjectAlternativeNames).append("]");
              }
            }
          }
        }
      } catch (Exception e) {
        logger.error("Unexpected error while getting SSL connection info for public access logger", e);
      }
      sslLogMessage.append(")");
    }
    logMessage.append(sslLogMessage);
  }
}
