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
   * The enabled protocols for SSLEngine
   */
  @Config("ssl.enabled.protocol")
  @Default("TLSv1.2")
  public final String sslEnabledProtocol;

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
  public final String sslKeyStoreType;

  /**
   * The SSL key store path
   */
  @Config("ssl.keystore.path")
  @Default("")
  public final String sslKeyStorePath;

  /**
   * The SSL key store password
   * There could be multiple keys in one key store
   * This password is to protect the integrity of the entire key store
   */
  @Config("ssl.keystore.password")
  @Default("")
  public final String sslKeyStorePassword;

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
  public final String sslTrustStoreType;

  /**
   * The SSL trust store path
   */
  @Config("ssl.truststore.path")
  @Default("")
  public final String sslTrustStorePath;

  /**
   * The SSL trust store password
   */
  @Config("ssl.truststore.password")
  @Default("")
  public final String sslTrustStorePassword;

  /**
   * The SSL supported cipher suites
   */
  @Config("ssl.cipher.suites")
  @Default("")
  public final String sslCipherSuites;

  public SSLConfig(VerifiableProperties verifiableProperties) {
    sslContextProtocol = verifiableProperties.getString("ssl.context.protocol", "TLS");
    sslContextProvider = verifiableProperties.getString("ssl.context.provider", "");
    sslEnabledProtocol = verifiableProperties.getString("ssl.enabled.protocol", "TLSv1.2");
    sslEndpointIdentificationAlgorithm =
        verifiableProperties.getString("ssl.endpoint.identification.algorithm", "");
    sslClientAuthentication = verifiableProperties.getString("ssl.client.authentication", "required");
    sslKeymanagerAlgorithm = verifiableProperties.getString("ssl.keymanager.algorithm", "");
    sslTrustmanagerAlgorithm = verifiableProperties.getString("ssl.trustmanager.algorithm", "");
    sslKeyStoreType = verifiableProperties.getString("ssl.keystore.type", "JKS");
    sslKeyStorePath = verifiableProperties.getString("ssl.keystore.path", "");
    sslKeyStorePassword = verifiableProperties.getString("ssl.keystore.password", "");
    sslKeyPassword = verifiableProperties.getString("ssl.key.password", "");
    sslTrustStoreType = verifiableProperties.getString("ssl.truststore.type", "JKS");
    sslTrustStorePath = verifiableProperties.getString("ssl.truststore.path", "");
    sslTrustStorePassword = verifiableProperties.getString("ssl.truststore.password", "");
    sslCipherSuites = verifiableProperties.getString("ssl.cipher.suites", "");
  }
}
