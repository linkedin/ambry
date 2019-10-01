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
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.conscrypt.Conscrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory to create SSLContext and SSLEngine
 */
public class JdkSslFactory implements SSLFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdkSslFactory.class);

  static {
    try {
      Conscrypt.checkAvailability();
      Security.addProvider(Conscrypt.newProvider());
    } catch (Throwable t) {
      // catching a throwable since Conscrypt.checkAvailability can throw an UnsatisfiedLinkError
      LOGGER.warn("Conscrypt not available for this platform; will not be able to use OpenSSL-based engine", t);
    }
  }

  private final SSLContext sslContext;
  private final String[] cipherSuites;
  private final String[] enabledProtocols;
  private final String endpointIdentification;
  private final ClientAuth clientAuth;

  /**
   * Construct an {@link JdkSslFactory}.
   * @param sslConfig the {@link SSLConfig} to use.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public JdkSslFactory(SSLConfig sslConfig) throws GeneralSecurityException, IOException {
    sslContext = createSSLContext(sslConfig);

    ArrayList<String> cipherSuitesList = Utils.splitString(sslConfig.sslCipherSuites, ",");
    cipherSuites = !cipherSuitesList.isEmpty() ? cipherSuitesList.toArray(new String[0]) : null;

    ArrayList<String> protocolsList = Utils.splitString(sslConfig.sslEnabledProtocols, ",");
    enabledProtocols = !protocolsList.isEmpty() ? protocolsList.toArray(new String[0]) : null;

    this.endpointIdentification =
        !sslConfig.sslEndpointIdentificationAlgorithm.isEmpty() ? sslConfig.sslEndpointIdentificationAlgorithm : null;

    switch (sslConfig.sslClientAuthentication) {
      case "required":
        clientAuth = ClientAuth.REQUIRED;
        break;
      case "requested":
        clientAuth = ClientAuth.REQUESTED;
        break;
      default:
        clientAuth = ClientAuth.NONE;
        break;
    }
  }

  /**
   * Create {@link SSLContext} by loading keystore and trustsotre
   * One factory only has one SSLContext
   * @param sslConfig the config for setting up the {@link SSLContext}
   * @return SSLContext
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private SSLContext createSSLContext(SSLConfig sslConfig) throws GeneralSecurityException, IOException {
    SSLContext sslContext;
    if (!sslConfig.sslContextProvider.isEmpty()) {
      sslContext = SSLContext.getInstance(sslConfig.sslContextProtocol, sslConfig.sslContextProvider);
    } else {
      sslContext = SSLContext.getInstance(sslConfig.sslContextProtocol);
    }

    SecurityStore keystore =
        new SecurityStore(sslConfig.sslKeystoreType, sslConfig.sslKeystorePath, sslConfig.sslKeystorePassword);
    String kmfAlgorithm = sslConfig.sslKeymanagerAlgorithm.isEmpty() ? KeyManagerFactory.getDefaultAlgorithm()
        : sslConfig.sslKeymanagerAlgorithm;
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
    KeyStore ks = keystore.load();
    String keyPassword = sslConfig.sslKeyPassword.isEmpty() ? keystore.password : sslConfig.sslKeyPassword;
    kmf.init(ks, keyPassword.toCharArray());
    KeyManager[] keyManagers = kmf.getKeyManagers();

    String tmfAlgorithm = sslConfig.sslTrustmanagerAlgorithm.isEmpty() ? TrustManagerFactory.getDefaultAlgorithm()
        : sslConfig.sslTrustmanagerAlgorithm;
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
    KeyStore ts = new SecurityStore(sslConfig.sslTruststoreType, sslConfig.sslTruststorePath,
        sslConfig.sslTruststorePassword).load();
    tmf.init(ts);

    sslContext.init(keyManagers, tmf.getTrustManagers(),
        sslConfig.sslSecureRandomAlgorithm.isEmpty() ? new SecureRandom()
            : SecureRandom.getInstance(sslConfig.sslSecureRandomAlgorithm));
    return sslContext;
  }

  /**
   * Create {@link SSLEngine} for given host name and port number.
   * This engine manages the handshake process and encryption/decryption with this remote host.
   * @param peerHost The remote host name
   * @param peerPort The remote port number
   * @param mode The local SSL mode, Client or Server
   * @return SSLEngine
   */
  @Override
  public SSLEngine createSSLEngine(String peerHost, int peerPort, Mode mode) {
    SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
    if (cipherSuites != null) {
      sslEngine.setEnabledCipherSuites(cipherSuites);
    }
    if (enabledProtocols != null) {
      sslEngine.setEnabledProtocols(enabledProtocols);
    }

    if (mode == Mode.SERVER) {
      sslEngine.setUseClientMode(false);
      switch (clientAuth) {
        case REQUIRED:
          sslEngine.setNeedClientAuth(true);
          break;
        case REQUESTED:
          sslEngine.setWantClientAuth(true);
          break;
      }
    } else {
      sslEngine.setUseClientMode(true);
      SSLParameters sslParams = sslEngine.getSSLParameters();
      sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
      sslEngine.setSSLParameters(sslParams);
    }
    return sslEngine;
  }

  /**
   * Returns a configured {@link SSLContext}. This context supports creating {@link SSLEngine}s and for the loaded
   * truststore and keystore. An {@link SSLEngine} must be created for each connection.
   * @return SSLContext.
   */
  @Override
  public SSLContext getSSLContext() {
    return sslContext;
  }

  private class SecurityStore {
    private final String type;
    private final String path;
    private final String password;

    private SecurityStore(String type, String path, String password) {
      this.type = type == null ? KeyStore.getDefaultType() : type;
      this.path = path;
      this.password = password;
    }

    private KeyStore load() throws GeneralSecurityException, IOException {
      try (FileInputStream in = new FileInputStream(path)) {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(in, password.toCharArray());
        return ks;
      }
    }
  }

  /**
   * The client authentication setting for server SSL engines.
   */
  private enum ClientAuth {
    REQUIRED, REQUESTED, NONE
  }
}
