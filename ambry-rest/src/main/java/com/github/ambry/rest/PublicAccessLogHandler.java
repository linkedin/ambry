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
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.SSLEngine;
import javax.security.auth.x500.X500Principal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.StringMapMessage;


/**
 * Captures headers and other required info from request and responses, to make public access log entries
 * {@link PublicAccessLogger} assists in logging the required information
 */
public class PublicAccessLogHandler extends ChannelDuplexHandler {
  private static final int SAN_DNS_FIELD_ID = 2;
  private final PublicAccessLogger publicAccessLogger;
  private final NettyMetrics nettyMetrics;
  private long requestArrivalTimeInMs;
  private long requestLastChunkArrivalTimeInMs;
  private long responseFirstChunkStartTimeInMs;
  private long responseBytesSent;
  private StringBuilder logMessage;
  private StringMapMessage structuredLogMessage;
  private HttpRequest request;
  private StringBuilder sslLogMessage;

  private static final long INIT_TIME = -1;
  //private static final Logger logger = LoggerFactory.getLogger(PublicAccessLogHandler.class);
  private static final Logger logger = LogManager.getLogger(PublicAccessLogHandler.class);

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
      structuredLogMessage.put("remoteAddress", String.valueOf(ctx.channel().remoteAddress()));
      logMessage.append(request.method().toString()).append(" ");
      structuredLogMessage.put("method", String.valueOf(request.method()));
      logMessage.append(request.uri()).append(", ");
      structuredLogMessage.put("uri", String.valueOf(request.uri()));
      logSSLInfo(ctx);
      logMessage.append(", ");
      logHeaders("Request", request, publicAccessLogger.getRequestHeaders());
      logMessage.append(", ");
    } else if (obj instanceof LastHttpContent) {
      requestLastChunkArrivalTimeInMs = System.currentTimeMillis();
    } else if (!(obj instanceof HttpContent)) {
      logger.error(
          "Receiving request (messageReceived) that is not of type HttpRequest or HttpContent. Receiving request from {}. Request is of type {}. No action being taken other than logging this unexpected state.",
          ctx.channel().remoteAddress(), obj.getClass());
    }
    nettyMetrics.publicAccessLogRequestProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
    super.channelRead(ctx, obj);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    long startTimeInMs = System.currentTimeMillis();
    boolean shouldReset = msg instanceof LastHttpContent;
    boolean isHttpContent = msg instanceof HttpContent;
    if (request != null) {
      if (msg instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) msg;
        logHeaders("Response", response, publicAccessLogger.getResponseHeaders());
        logMessage.append(", ");
        logMessage.append("status=").append(response.status().code());
        logMessage.append(", ");
        structuredLogMessage.put("status", String.valueOf(response.status().code()));
        if (HttpUtil.isTransferEncodingChunked(response)) {
          responseFirstChunkStartTimeInMs = System.currentTimeMillis();
        } else {
          shouldReset = true;
        }
      } else if (!(msg instanceof HttpContent)) {
        logger.error(
            "Sending response that is not of type HttpResponse or HttpContent. Sending response to {}. Request is of type {}. No action being taken other than logging this unexpected state.",
            ctx.channel().remoteAddress(), msg.getClass());
      }
      if (isHttpContent) {
        HttpContent httpContent = (HttpContent) msg;
        responseBytesSent += httpContent.content().readableBytes();
      }
      if (shouldReset) {
        logDurations();

        if (publicAccessLogger.structuredLoggingEnabled()) {
          // Write out the old log message as a full string, as well as add each field individually.
          structuredLogMessage.put("message", logMessage.toString());
          publicAccessLogger.logInfo(structuredLogMessage);
        } else {
          publicAccessLogger.logInfo(logMessage.toString());
        }
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
        structuredLogMessage.put(header, message.headers().get(header));
      }
    }
    boolean isChunked = HttpUtil.isTransferEncodingChunked(message);
    logMessage.append("[isChunked=").append(isChunked).append("]");
    logMessage.append(")");
    structuredLogMessage.put("isChunked", String.valueOf(isChunked));
  }

  /**
   * Appends duration of operation to the log message. Also appends duration of receive or send phases of
   * operations if those phases used chunked encoding.
   */
  private void logDurations() {
    long nowMs = System.currentTimeMillis();
    logMessage.append("bytes sent=").append(responseBytesSent).append(" ");
    structuredLogMessage.put("bytesSent", String.valueOf(responseBytesSent));
    logMessage.append("duration=").append(nowMs - requestArrivalTimeInMs).append("ms ");
    structuredLogMessage.put("duration", String.valueOf(nowMs - requestArrivalTimeInMs).concat("ms"));
    if (requestLastChunkArrivalTimeInMs != INIT_TIME) {
      logMessage.append("(chunked request receive=")
          .append(requestLastChunkArrivalTimeInMs - requestArrivalTimeInMs)
          .append("ms) ");
      structuredLogMessage.put("chunkedRequestReceive",
          String.valueOf(requestLastChunkArrivalTimeInMs - requestArrivalTimeInMs).concat("ms"));
    }
    if (responseFirstChunkStartTimeInMs != INIT_TIME) {
      logMessage.append("(chunked response send=").append(nowMs - responseFirstChunkStartTimeInMs).append("ms) ");
      structuredLogMessage.put("chunkedResponseSend",
          String.valueOf(nowMs - responseFirstChunkStartTimeInMs).concat("ms"));
    }
  }

  /**
   * Resets some variables as part of logging a response
   */
  private void reset() {
    responseFirstChunkStartTimeInMs = INIT_TIME;
    requestLastChunkArrivalTimeInMs = INIT_TIME;
    requestArrivalTimeInMs = INIT_TIME;
    responseBytesSent = 0;
    logMessage = new StringBuilder();
    structuredLogMessage = new StringMapMessage();
    request = null;
  }

  /**
   * Logs error message
   * @param msg the message to log
   */
  private void logError(String msg) {
    logDurations();
    logMessage.append(msg);
    structuredLogMessage.put("error", msg);

    if (publicAccessLogger.structuredLoggingEnabled()) {
      publicAccessLogger.logError(structuredLogMessage);
    } else {
      publicAccessLogger.logError(logMessage.toString());
    }
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
        structuredLogMessage.put("used", String.valueOf(sslUsed));

        if (sslUsed) {
          SSLEngine sslEngine = sslHandler.engine();
          if (sslEngine.getNeedClientAuth()) {
            for (Certificate certificate : sslEngine.getSession().getPeerCertificates()) {
              if (certificate instanceof X509Certificate) {
                X500Principal principal = ((X509Certificate) certificate).getSubjectX500Principal();

                sslLogMessage.append(", [principal=").append(principal).append("]");
                structuredLogMessage.put("principal", principal.toString());
                Collection<List<?>> subjectAlternativeNames =
                    ((X509Certificate) certificate).getSubjectAlternativeNames();
                if (subjectAlternativeNames == null || subjectAlternativeNames.isEmpty()) {
                  continue;
                }
                /*
                 * https://docs.oracle.com/javase/7/docs/api/java/security/cert/X509Certificate.html#getSubjectAlternativeNames()
                 * For getSubjectAlternativeNames, a Collection is returned with an entry representing each GeneralName included in the extension.
                 * Each entry is a List whose first entry is an Integer (the name type, 0-8) and whose second entry is a String or a byte array.
                 * */
                // here we filter out dns field and value, we don't need those values in public access log.
                List<List<?>> sans = subjectAlternativeNames.stream()
                    .filter(p -> (Integer) p.get(0) != SAN_DNS_FIELD_ID)
                    .collect(Collectors.toList());
                sslLogMessage.append(", [san=").append(sans).append("]");
                structuredLogMessage.put("san", String.valueOf(sans));
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
