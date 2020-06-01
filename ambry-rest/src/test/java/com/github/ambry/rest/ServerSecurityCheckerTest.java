/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.router.Callback;
import com.github.ambry.server.ServerSecurityService;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.junit.Assert;
import org.junit.Test;


public class ServerSecurityCheckerTest {

  private static final X509Certificate PEER_CERT;
  private static final SslContext SSL_CONTEXT;

  private ServerSecurityService serverSecurityService;

  static {
    try {
      PEER_CERT = new SelfSignedCertificate().cert();
      SelfSignedCertificate localCert = new SelfSignedCertificate();
      SSL_CONTEXT = SslContextBuilder.forServer(localCert.certificate(), localCert.privateKey())
          .clientAuth(ClientAuth.REQUIRE)
          .build();
    } catch (CertificateException | SSLException e) {
      throw new IllegalStateException(e);
    }
  }

  public ServerSecurityCheckerTest() {
    serverSecurityService = new MockServerSecurityService();
  }

  @Test
  public void securityCheckerTest() throws Exception {
    //success case, channel should not be closed.
    EmbeddedChannel channel = createChannelSsl();
    SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
    Promise<Channel> promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    Assert.assertTrue("channel should not be closed", channel.isActive());

    //failure case, channel should be closed.
    serverSecurityService.close();
    channel = createChannelSsl();
    sslHandler = channel.pipeline().get(SslHandler.class);
    promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    Assert.assertTrue("channel should be closed", !channel.isActive());
  }

  //helpers
  //general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link ServerSecurityChecker}
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link ServerSecurityChecker}
   *         and an {@link SslHandler}.
   */
  private EmbeddedChannel createChannelSsl() {
    SSLEngine sslEngine = SSL_CONTEXT.newEngine(ByteBufAllocator.DEFAULT);
    SSLEngine mockSSLEngine =
        new MockSSLEngine(sslEngine, new MockSSLSession(sslEngine.getSession(), new Certificate[]{PEER_CERT}));
    SslHandler sslHandler = new SslHandler(mockSSLEngine);
    ServerSecurityChecker serverSecurityChecker = new ServerSecurityChecker(serverSecurityService);
    EmbeddedChannel channel = new EmbeddedChannel(sslHandler, serverSecurityChecker);
    return channel;
  }
}

class MockServerSecurityService implements ServerSecurityService {
  boolean isOpen = true;

  @Override
  public void validateConnection(ChannelHandlerContext ctx, Callback<Void> callback) {
    Exception exception = null;
    if (!isOpen) {
      exception = new RestServiceException("ServerSecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    }
    callback.onCompletion(null, exception);
  }

  @Override
  public void validateRequest(RestRequest restRequest, Callback<Void> callback) {

  }

  @Override
  public void close() throws IOException {
    isOpen = false;
  }
}
