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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.rest.MockSSLEngine;
import com.github.ambry.rest.MockSSLSession;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.Promise;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link Http2PeerCertificateValidator}
 */
public class Http2PeerCertificateValidatorTest {
  /**
   * Sets up the mock securityservice {@link Http2PeerCertificateValidator} to use.
   */
  public Http2PeerCertificateValidatorTest() {

  }

  /**
   * Test the code flow where {@link Http2PeerCertificateValidator} waits for the SSL handshake complete event and
   * apply security policy from channelActive method.
   * @throws Exception
   */
  @Test
  public void securityCheckerTest() throws Exception {

    GeneralNames subjectAltNames = new GeneralNames(new GeneralName[]{
        new GeneralName(GeneralName.uniformResourceIdentifier,
            "urn:li:servicePrincipal(foo-name;foo-tag;i001)"),
        new GeneralName(GeneralName.dNSName, "ca-00001.foo.com"),
        new GeneralName(GeneralName.iPAddress, "1.1.1.1")});

    //certificate validation success case, channel should not be closed.
    Http2ClientMetrics metrics = new Http2ClientMetrics(new MetricRegistry());
    EmbeddedChannel channel = createChannelSsl(subjectAltNames, "urn:li:servicePrincipal\\(foo-name;foo-tag;.*\\) ?", metrics);
    SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
    Promise<Channel> promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    assertTrue("channel should not be closed", channel.isActive());
    assertEquals("validation success counter mismatch", 0,
        metrics.http2ServerCertificateValidationFailureCount.getCount());

    //certificate validation is disabled with empty regex, channel should not be closed.
    channel = createChannelSsl(subjectAltNames, "", metrics);
    sslHandler = channel.pipeline().get(SslHandler.class);
    promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    assertTrue("channel should not be closed", channel.isActive());
    assertEquals("validation success counter mismatch", 0,
        metrics.http2ServerCertificateValidationFailureCount.getCount());

    //certificate validation is disabled with empty regex, channel should not be closed.
    channel = createChannelSsl(subjectAltNames, " ", metrics);
    sslHandler = channel.pipeline().get(SslHandler.class);
    promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    assertTrue("channel should not be closed", channel.isActive());
    assertEquals("validation success counter mismatch", 0,
        metrics.http2ServerCertificateValidationFailureCount.getCount());

    //security validation failure case (service closed), channel should be closed.
    channel = createChannelSsl(subjectAltNames, "invalid.*", metrics);
    sslHandler = channel.pipeline().get(SslHandler.class);
    promise = (Promise<Channel>) sslHandler.handshakeFuture();
    promise.setSuccess(channel);
    assertTrue("channel should be closed", !channel.isActive());
    assertEquals("validation success counter mismatch", 1,
        metrics.http2ServerCertificateValidationFailureCount.getCount());
  }

  //helpers
  //general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link Http2PeerCertificateValidator}
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link Http2PeerCertificateValidator}
   *         and an {@link SslHandler}.
   */
  private EmbeddedChannel createChannelSsl(GeneralNames subjectAltNames, String regex,
      Http2ClientMetrics http2ClientMetrics) {
    SslContext sslContext;
    X509Certificate cert;

    try {
      KeyPair cKP = TestSSLUtils.generateKeyPair("RSA");
      cert = TestSSLUtils.generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA",
          Optional.of(subjectAltNames));

      SelfSignedCertificate localCert = new SelfSignedCertificate();
      sslContext = SslContextBuilder.forServer(localCert.certificate(), localCert.privateKey())
          .clientAuth(ClientAuth.REQUIRE)
          .build();
    } catch (CertificateException | SSLException | NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    SSLEngine sslEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT);
    SSLEngine mockSSLEngine =
        new MockSSLEngine(sslEngine, new MockSSLSession(sslEngine.getSession(), new Certificate[]{cert}));
    SslHandler sslHandler = new SslHandler(mockSSLEngine);
    EmbeddedChannel channel = new EmbeddedChannel(sslHandler, new Http2PeerCertificateValidator(regex, http2ClientMetrics));
    return channel;
  }
}

