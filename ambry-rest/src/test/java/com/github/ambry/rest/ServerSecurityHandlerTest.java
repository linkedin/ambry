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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.commons.Callback;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
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
import javax.net.ssl.SSLSession;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit tests for {@link ServerSecurityHandler}
 */
public class ServerSecurityHandlerTest {

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

  /**
   * Sets up the mock securityservice {@link ServerSecurityService} to use.
   */
  public ServerSecurityHandlerTest() {
    serverSecurityService = new MockServerSecurityService();
  }

  /**
   * Test the code flow where {@link ServerSecurityHandler} waits for the SSL handshake complete event and
   * apply security policy from channelActive method.
   * @throws Exception
   */
  @Test
  public void securityCheckerTest() throws Exception {
    //secuirty validation success case, channel should not be closed.
    ServerMetrics metrics = new ServerMetrics(new MetricRegistry(), this.getClass(), this.getClass());
    EmbeddedChannel channel = createChannelSsl(metrics);
    SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
    Promise<Channel> promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    Assert.assertTrue("channel should not be closed", channel.isActive());
    Assert.assertEquals("validation success counter mismatch", 1,
        metrics.serverValidateConnectionSuccess.getCount());


    //security validation failure case (throw exception), channel should be closed.
    MockServerSecurityService mockServerSecurityService = (MockServerSecurityService)serverSecurityService;
    mockServerSecurityService.setThrowException(true);
    metrics = new ServerMetrics(new MetricRegistry(), this.getClass(), this.getClass());
    channel = createChannelSsl(metrics);
    sslHandler = channel.pipeline().get(SslHandler.class);
    promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    Assert.assertTrue("channel should be closed", !channel.isActive());
    Assert.assertEquals("validation success counter mismatch", 1,
        metrics.serverValidateConnectionFailure.getCount());

    //security validation failure case (service closed), channel should be closed.
    serverSecurityService.close();
    metrics = new ServerMetrics(new MetricRegistry(), this.getClass(), this.getClass());
    channel = createChannelSsl(metrics);
    sslHandler = channel.pipeline().get(SslHandler.class);
    promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    Assert.assertTrue("channel should be closed", !channel.isActive());
    Assert.assertEquals("validation success counter mismatch", 1,
        metrics.serverValidateConnectionFailure.getCount());



  }

  //helpers
  //general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link ServerSecurityHandler}
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link ServerSecurityHandler}
   *         and an {@link SslHandler}.
   */
  private EmbeddedChannel createChannelSsl(ServerMetrics metrics) {
    SSLEngine sslEngine = SSL_CONTEXT.newEngine(ByteBufAllocator.DEFAULT);
    SSLEngine mockSSLEngine =
        new MockSSLEngine(sslEngine, new MockSSLSession(sslEngine.getSession(), new Certificate[]{PEER_CERT}));
    SslHandler sslHandler = new SslHandler(mockSSLEngine);
    ServerSecurityHandler serverSecurityHandler = new ServerSecurityHandler(serverSecurityService, metrics);
    EmbeddedChannel channel = new EmbeddedChannel(sslHandler, serverSecurityHandler);
    return channel;
  }
}

/**
 * A mock class for {@link ServerSecurityService}
 */
class MockServerSecurityService implements ServerSecurityService {
  private boolean isOpen = true;
  private boolean throwException = false;

  public void setThrowException(boolean throwException) {
    this.throwException = throwException;
  }

  @Override
  public void validateConnection(SSLSession sslSession, Callback<Void> callback) {
    Exception exception = null;
    if (!isOpen) {
      exception = new RestServiceException("ServerSecurityService is closed", RestServiceErrorCode.ServiceUnavailable);
    } else if (throwException) {
      throw new IllegalArgumentException("dummy exception for testing purpose");
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
