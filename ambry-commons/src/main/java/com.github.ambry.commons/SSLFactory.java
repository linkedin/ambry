/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import java.util.ArrayList;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory to create SSLContext and SSLEngine
 */
public class SSLFactory {
  public enum Mode {CLIENT, SERVER}

  protected static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

  private String protocol;
  private String provider;
  private String kmfAlgorithm;
  private String tmfAlgorithm;
  private SecurityStore keystore;
  private String keyPassword;
  private SecurityStore truststore;
  private String[] cipherSuites;
  private String[] enabledProtocols;
  private String endpointIdentification;
  private SSLContext sslContext;
  private boolean needClientAuth;
  private boolean wantClientAuth;

  /**
   * Construct an {@link SSLFactory}.
   * @param sslConfig the {@link SSLConfig} to use.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public SSLFactory(SSLConfig sslConfig) throws GeneralSecurityException, IOException {

    this.protocol = sslConfig.sslContextProtocol;
    if (sslConfig.sslContextProvider.length() > 0) {
      this.provider = sslConfig.sslContextProvider;
    }

    ArrayList<String> cipherSuitesList = Utils.splitString(sslConfig.sslCipherSuites, ",");
    if (cipherSuitesList != null && cipherSuitesList.size() > 0 && !(cipherSuitesList.size() == 1
        && cipherSuitesList.get(0).equals(""))) {
      this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
    }

    ArrayList<String> protocolsList = Utils.splitString(sslConfig.sslEnabledProtocols, ",");
    if (protocolsList != null && protocolsList.size() > 0) {
      this.enabledProtocols = protocolsList.toArray(new String[protocolsList.size()]);
    }

    if (sslConfig.sslEndpointIdentificationAlgorithm.length() > 0
        && !sslConfig.sslEndpointIdentificationAlgorithm.equals("")) {
      this.endpointIdentification = sslConfig.sslEndpointIdentificationAlgorithm;
    }

    if (sslConfig.sslClientAuthentication.equals("required")) {
      this.needClientAuth = true;
    } else if (sslConfig.sslClientAuthentication.equals("requested")) {
      this.wantClientAuth = true;
    }

    if (sslConfig.sslKeymanagerAlgorithm.length() > 0) {
      this.kmfAlgorithm = sslConfig.sslKeymanagerAlgorithm;
    }

    if (sslConfig.sslTrustmanagerAlgorithm.length() > 0) {
      this.tmfAlgorithm = sslConfig.sslTrustmanagerAlgorithm;
    }

    createKeyStore(sslConfig.sslKeystoreType, sslConfig.sslKeystorePath, sslConfig.sslKeystorePassword,
        sslConfig.sslKeyPassword);
    createTrustStore(sslConfig.sslTruststoreType, sslConfig.sslTruststorePath, sslConfig.sslTruststorePassword);

    this.sslContext = createSSLContext();
  }

  /**
   * Create {@link SSLContext} by loading keystore and trustsotre
   * One factory only has one SSLContext
   * @return SSLContext
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private SSLContext createSSLContext() throws GeneralSecurityException, IOException {
    SSLContext sslContext;
    if (provider != null) {
      sslContext = SSLContext.getInstance(protocol, provider);
    } else {
      sslContext = SSLContext.getInstance(protocol);
    }

    KeyManager[] keyManagers = null;
    if (keystore != null) {
      String kmfAlgorithm = this.kmfAlgorithm != null ? this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
      KeyStore ks = keystore.load();
      String keyPassword = this.keyPassword != null ? this.keyPassword : keystore.password;
      kmf.init(ks, keyPassword.toCharArray());
      keyManagers = kmf.getKeyManagers();
    }

    String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
    KeyStore ts = truststore == null ? null : truststore.load();
    tmf.init(ts);

    sslContext.init(keyManagers, tmf.getTrustManagers(), new SecureRandom());
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
      if (needClientAuth) {
        sslEngine.setNeedClientAuth(needClientAuth);
      } else {
        sslEngine.setWantClientAuth(wantClientAuth);
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
  public SSLContext getSSLContext() {
    return sslContext;
  }

  private void createKeyStore(String type, String path, String password, String keyPassword) {
    this.keystore = new SecurityStore(type, path, password);
    this.keyPassword = keyPassword;
  }

  private void createTrustStore(String type, String path, String password) {
    this.truststore = new SecurityStore(type, path, password);
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
      FileInputStream in = null;
      try {
        KeyStore ks = KeyStore.getInstance(type);
        in = new FileInputStream(path);
        ks.load(in, password.toCharArray());
        return ks;
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }
  }
}
