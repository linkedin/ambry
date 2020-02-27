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

package com.github.ambry.commons;

import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
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
public class NettySslFactory implements SSLFactory {
  private static final Logger logger = LoggerFactory.getLogger(NettySslFactory.class);
  private final SslContext nettyServerSslContext;
  private final SslContext nettyClientSslContext;
  private final SSLContext jdkSslContext;
  private final String endpointIdentification;

  /**
   * Instantiate a {@link NettySslFactory} from a config.
   * @param sslConfig the {@link SSLConfig} to use.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public NettySslFactory(SSLConfig sslConfig) throws GeneralSecurityException, IOException {
    nettyServerSslContext = getServerSslContext(sslConfig);
    nettyClientSslContext = getClientSslContext(sslConfig);
    // Netty's OpenSsl based implementation does not use the JDK SSLContext so we have to fall back to the JDK based
    // factory to support this method.
    jdkSslContext = new JdkSslFactory(sslConfig).getSSLContext();

    this.endpointIdentification =
        sslConfig.sslEndpointIdentificationAlgorithm.isEmpty() ? null : sslConfig.sslEndpointIdentificationAlgorithm;
  }

  @Override
  public SSLEngine createSSLEngine(String peerHost, int peerPort, Mode mode) {
    SslContext context = mode == Mode.CLIENT ? nettyClientSslContext : nettyServerSslContext;
    SSLEngine sslEngine = context.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort);

    if (mode == Mode.CLIENT) {
      SSLParameters sslParams = sslEngine.getSSLParameters();
      sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
      sslEngine.setSSLParameters(sslParams);
    }
    return sslEngine;
  }

  @Override
  public SSLContext getSSLContext() {
    return jdkSslContext;
  }

  /**
   * @param config the {@link SSLConfig}
   * @return a configured {@link SslContext} object for a client.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static SslContext getServerSslContext(SSLConfig config) throws GeneralSecurityException, IOException {
    logger.info("Using {} provider for server SslContext", SslContext.defaultServerProvider());
    return SslContextBuilder.forServer(getKeyManagerFactory(config))
        .trustManager(getTrustManagerFactory(config))
        .ciphers(getCipherSuites(config))
        .protocols(getEnabledProtocols(config))
        .clientAuth(getClientAuth(config))
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
  static KeyManagerFactory getKeyManagerFactory(SSLConfig config) throws GeneralSecurityException, IOException {
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
  static TrustManagerFactory getTrustManagerFactory(SSLConfig config)
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
  static String[] getEnabledProtocols(SSLConfig config) {
    List<String> enabledProtocols = Utils.splitString(config.sslEnabledProtocols, ",");
    return !enabledProtocols.isEmpty() ? enabledProtocols.toArray(new String[0]) : null;
  }

  /**
   * @param config the {@link SSLConfig}.
   * @return the {@link ClientAuth} setting.
   */
  static ClientAuth getClientAuth(SSLConfig config) {
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
