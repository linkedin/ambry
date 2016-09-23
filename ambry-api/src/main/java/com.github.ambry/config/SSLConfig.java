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
package com.github.ambry.config;

/**
 * The configs for SSL
 */
public class SSLConfig {
  /**
   * The SSL protocol for SSLContext
   */
  @Config("ssl.context.protocol")
  @Default("TLS")
  public final String sslContextProtocol;

  /**
   * The SSL provider for SSLContext
   */
  @Config("ssl.context.provider")
  @Default("")
  public final String sslContextProvider;

  /**
   * The enabled protocols for SSLEngine, a comma separated list of values
   */
  @Config("ssl.enabled.protocols")
  @Default("TLSv1.2")
  public final String sslEnabledProtocols;

  /**
   * The SSL endpoint identification algorithm
   */
  @Config("ssl.endpoint.identification.algorithm")
  @Default("")
  public final String sslEndpointIdentificationAlgorithm;

  /**
   * The SSL client authentication config
   */
  @Config("ssl.client.authentication")
  @Default("required")
  public final String sslClientAuthentication;

  /**
   * The SSL keymanager algorithm
   */
  @Config("ssl.keymanager.algorithm")
  @Default("")
  public final String sslKeymanagerAlgorithm;

  /**
   * The SSL trustmanager algorithm
   */
  @Config("ssl.trustmanager.algorithm")
  @Default("")
  public final String sslTrustmanagerAlgorithm;

  /**
   * The SSL key store type
   */
  @Config("ssl.keystore.type")
  @Default("JKS")
  public final String sslKeystoreType;

  /**
   * The SSL key store path
   */
  @Config("ssl.keystore.path")
  @Default("")
  public final String sslKeystorePath;

  /**
   * The SSL key store password
   * There could be multiple keys in one key store
   * This password is to protect the integrity of the entire key store
   */
  @Config("ssl.keystore.password")
  @Default("")
  public final String sslKeystorePassword;

  /**
   * The SSL key password
   * The key store protects each private key with its individual password
   */
  @Config("ssl.key.password")
  @Default("")
  public final String sslKeyPassword;

  /**
   * The SSL trust store type
   */
  @Config("ssl.truststore.type")
  @Default("JKS")
  public final String sslTruststoreType;

  /**
   * The SSL trust store path
   */
  @Config("ssl.truststore.path")
  @Default("")
  public final String sslTruststorePath;

  /**
   * The SSL trust store password
   */
  @Config("ssl.truststore.password")
  @Default("")
  public final String sslTruststorePassword;

  /**
   * The SSL supported cipher suites, a comma separated list of values
   */
  @Config("ssl.cipher.suites")
  @Default("")
  public final String sslCipherSuites;

  public SSLConfig(VerifiableProperties verifiableProperties) {
    sslContextProtocol = verifiableProperties.getString("ssl.context.protocol", "TLS");
    sslContextProvider = verifiableProperties.getString("ssl.context.provider", "");
    sslEnabledProtocols = verifiableProperties.getString("ssl.enabled.protocols", "TLSv1.2");
    sslEndpointIdentificationAlgorithm = verifiableProperties.getString("ssl.endpoint.identification.algorithm", "");
    sslClientAuthentication = verifiableProperties.getString("ssl.client.authentication", "required");
    sslKeymanagerAlgorithm = verifiableProperties.getString("ssl.keymanager.algorithm", "");
    sslTrustmanagerAlgorithm = verifiableProperties.getString("ssl.trustmanager.algorithm", "");
    sslKeystoreType = verifiableProperties.getString("ssl.keystore.type", "JKS");
    sslKeystorePath = verifiableProperties.getString("ssl.keystore.path", "");
    sslKeystorePassword = verifiableProperties.getString("ssl.keystore.password", "");
    sslKeyPassword = verifiableProperties.getString("ssl.key.password", "");
    sslTruststoreType = verifiableProperties.getString("ssl.truststore.type", "JKS");
    sslTruststorePath = verifiableProperties.getString("ssl.truststore.path", "");
    sslTruststorePassword = verifiableProperties.getString("ssl.truststore.password", "");
    sslCipherSuites = verifiableProperties.getString("ssl.cipher.suites", "");
  }
}
