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
package com.github.ambry.network.http2;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.net.ssl.SSLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler that validates the peer certificate on the new HTTP2 connection.
 */
@ChannelHandler.Sharable
public class Http2PeerCertificateValidator extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(Http2PeerCertificateValidator.class);
  private final String http2PeerCertificateSanRegex;
  private final Http2ClientMetrics http2ClientMetrics;
  private final Pattern http2PeerCertificateSanRegexPattern;

  public Http2PeerCertificateValidator(String http2PeerCertificateSanRegex,
      Http2ClientMetrics http2ClientMetrics) throws PatternSyntaxException {
    this.http2PeerCertificateSanRegex = http2PeerCertificateSanRegex;
    this.http2PeerCertificateSanRegexPattern = Pattern.compile(http2PeerCertificateSanRegex);
    this.http2ClientMetrics = http2ClientMetrics;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Channel Active " + ctx.channel().remoteAddress());
    validateSslConnection(ctx);
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Channel Inactive {}", ctx.channel().remoteAddress());
    super.channelInactive(ctx);
  }

  /**
   * Listen to the handshake future for the SSL termination, then apply the security policies, potentialy closing the
   * connection in case security check fails.
   * @param ctx the {@link ChannelHandlerContext}.
   */
  private void validateSslConnection(ChannelHandlerContext ctx) throws Exception {
    if (http2PeerCertificateSanRegex.trim().isEmpty()) {
      return;
    }
    SslHandler sslHandler = ctx.channel().pipeline().get(SslHandler.class);
    if (sslHandler == null) {
      return;
    }
    sslHandler.handshakeFuture().addListener(future -> {
      if (!future.isSuccess()) {
        logger.error("SSL handshake failed for channel: {}", ctx.channel(), future.cause());
        return;
      }
      try {
        SSLSession sslSession = sslHandler.engine().getSession();
        Certificate[] peerCertificates = sslSession.getPeerCertificates();
        for (Certificate cert : peerCertificates) {
          if (cert instanceof X509Certificate) {
            X509Certificate x509Certificate = (X509Certificate) cert;
            Principal p = x509Certificate.getSubjectDN();

            Collection<?> subjectAltNames = x509Certificate.getSubjectAlternativeNames();
            if (subjectAltNames == null) {
              continue;
            }
            int numSanName = 0;
            for (Object subjectAltName : subjectAltNames) {
              numSanName++;
              List<?> entry = (List<?>) subjectAltName;
              if (entry == null || entry.size() < 2) {
                continue;
              }
              Integer altNameType = (Integer) entry.get(0);
              String altName = (String) entry.get(1);
              if (altNameType == null || altName == null) {
                continue;
              }

              if (http2PeerCertificateSanRegexPattern.matcher(altName).matches()) {
                return;
              }
            }
          }
        }
      } catch (Exception e) {
        logger.error("Exception thrown, peer certificate validation failed for channel: {}", ctx.channel(), e);
      }
      logger.error("Peer certificate validation failed for channel: {}", ctx.channel());
      http2ClientMetrics.http2ServerCertificateValidationFailureCount.inc();
      ctx.channel().close();
    });
  }
}
