/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.JdkSslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link SSLFactory} that uses Netty's SSL libraries. This has the benefit of using OpenSSL
 * instead of JDK's SSL implementation when the netty-tcnative library is loaded. OpenSSL shows
 * significant performance enhancements over the JDK implementation.
 */
public class NettySslHttp2Factory extends NettySslFactory {
  private static final Logger logger = LoggerFactory.getLogger(NettySslHttp2Factory.class);
  /**
   * Instantiate a {@link NettySslHttp2Factory} from a config.
   * @param sslConfig the {@link SSLConfig} to use.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public NettySslHttp2Factory(SSLConfig sslConfig) throws GeneralSecurityException, IOException {
   super(sslConfig);
  }

  @Override
  private static SslContext getServerSslContext(SSLConfig config) throws GeneralSecurityException, IOException {
    logger.info("Using {} provider for server SslContext", SslContext.defaultServerProvider());
    return SslContextBuilder.forServer(getKeyManagerFactory(config))
        .trustManager(getTrustManagerFactory(config))
        .protocols(getEnabledProtocols(config))
        .clientAuth(getClientAuth(config))
        /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
         * Please refer to the HTTP/2 specification for cipher requirements. */
        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
        .applicationProtocolConfig(new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            ApplicationProtocolNames.HTTP_2
        ))
        .build();
  }

  /**
   * @param config the {@link SSLConfig}
   * @return a configured {@link SslContext} object for a server.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static SslContext getClientSslContext(SSLConfig config) throws GeneralSecurityException, IOException {
    logger.info("Using {} provider for client SslContext", SslContext.defaultClientProvider());
    return SslContextBuilder.forClient()
        .keyManager(getKeyManagerFactory(config))
        .trustManager(getTrustManagerFactory(config))
        .ciphers(getCipherSuites(config))
        .protocols(getEnabledProtocols(config))
        .build();
  }

  /**
   * Load a {@link KeyStore} from disk.
   * @param storePath the file path to the key store.
   * @param storeType the key store provider type.
   * @param storePassword the password for the key store.
   * @return an instantiated {@link KeyStore}.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static KeyStore loadKeyStore(String storePath, String storeType, String storePassword)
      throws GeneralSecurityException, IOException {
    try (FileInputStream in = new FileInputStream(storePath)) {
      KeyStore ks = KeyStore.getInstance(storeType);
      ks.load(in, storePassword.toCharArray());
      return ks;
    }
  }

  /**
   * @param config the {@link SSLConfig}.
   * @return an initialized {@link KeyManagerFactory}
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static KeyManagerFactory getKeyManagerFactory(SSLConfig config) throws GeneralSecurityException, IOException {
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    KeyStore ks = loadKeyStore(config.sslKeystorePath, config.sslKeystoreType, config.sslKeystorePassword);
    String keyPassword = config.sslKeyPassword.isEmpty() ? config.sslKeystorePassword : config.sslKeyPassword;
    kmf.init(ks, keyPassword.toCharArray());
    return kmf;
  }

  /**
   * @param config the {@link SSLConfig}.
   * @return an initialized {@link TrustManagerFactory}
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static TrustManagerFactory getTrustManagerFactory(SSLConfig config)
      throws GeneralSecurityException, IOException {
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    KeyStore ks = loadKeyStore(config.sslTruststorePath, config.sslTruststoreType, config.sslTruststorePassword);
    tmf.init(ks);
    return tmf;
  }

  /**
   * @param config the {@link SSLConfig}.
   * @return the list of supported cipher suites, or {@code null} if the configs did not specify any
   */
  private static Iterable<String> getCipherSuites(SSLConfig config) {
    List<String> cipherSuitesList = Utils.splitString(config.sslCipherSuites, ",");
    return cipherSuitesList.size() > 0 ? cipherSuitesList : null;
  }

  /**
   * @param config the {@link SSLConfig}.
   * @return the list of supported cipher suites, or {@code null} if the configs did not specify any
   */
  private static String[] getEnabledProtocols(SSLConfig config) {
    List<String> enabledProtocols = Utils.splitString(config.sslEnabledProtocols, ",");
    return !enabledProtocols.isEmpty() ? enabledProtocols.toArray(new String[0]) : null;
  }

  /**
   * @param config the {@link SSLConfig}.
   * @return the {@link ClientAuth} setting.
   */
  private static ClientAuth getClientAuth(SSLConfig config) {
    switch (config.sslClientAuthentication) {
      case "required":
        return ClientAuth.REQUIRE;
      case "requested":
        return ClientAuth.OPTIONAL;
      default:
        return ClientAuth.NONE;
    }
  }
}
