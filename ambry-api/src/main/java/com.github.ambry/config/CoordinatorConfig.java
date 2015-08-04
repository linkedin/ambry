package com.github.ambry.config;

/**
 * The configs for Coordinator. This includes configs for operations as well as for coordinator.
 */
public class CoordinatorConfig {

  /**
   * The hostname of the node upon which the coordinator runs.
   */
  @Config("coordinator.hostname")
  public final String hostname;

  /**
   * The name of the datacenter in which the coordinator is located.
   */
  @Config("coordinator.datacenter.name")
  public final String datacenterName;

  /**
   * The number of threads in the requester thread pool.
   */
  @Config("coordinator.requester.pool.size")
  @Default("100")
  public final int requesterPoolSize;

  /**
   * Timeout for operations that the coordinator issues.
   */
  @Config("coordinator.operation.timeout.ms")
  @Default("2000")
  public final int operationTimeoutMs;

  /**
   * The factory class the coordinator uses to create a connection pool.
   */
  @Config("coordinator.connection.pool.factory")
  @Default("com.github.ambry.network.BlockingChannelConnectionPoolFactory")
  public final String connectionPoolFactory;

  /**
   * Timeout for checking out a connection from the connection pool
   */
  @Config("coordinator.connection.pool.checkout.timeout.ms")
  @Default("1000")
  public final int connectionPoolCheckoutTimeoutMs;

  /**
   * Indicates if all operations should or should not do cross dc proxy calls
   */
  @Config("coordinator.cross.dc.proxy.call.enable")
  @Default("true")
  public final boolean crossDCProxyCallEnable;

  /**
   * List of Datacenters to which we need SSL encryption
   */
  @Config("coordinator.ssl.enabled.datacenters")
  public final String sslEnabledDatacenters;

  /**
   * The SSL protocol
   */
  @Config("coordinator.ssl.protocol")
  @Default("TLS")
  public final String sslProtocol;

  /**
   * The SSL key store type
   */
  @Config("coordinator.ssl.keystore.type")
  @Default("JKS")
  public final String sslKeyStoreType;

  /**
   * The SSL key store path
   */
  @Config("coordinator.ssl.keystore.path")
  @Default("selfsigned-keystore.jks")
  public final String sslKeyStorePath;

  /**
   * The SSL key store password
   */
  @Config("coordinator.ssl.keystore.password")
  @Default("unittestonly")
  public final String sslKeyStorePassword;

  /**
   * The SSL key password
   */
  @Config("coordinator.ssl.key.password")
  @Default("unittestonly")
  public final String sslKeyPassword;

  /**
   * The SSL trust store type
   */
  @Config("coordinator.ssl.truststore.type")
  @Default("JKS")
  public final String sslTrustStoreType;

  /**
   * The SSL trust store path
   */
  @Config("coordinator.ssl.truststore.path")
  @Default("selfsigned-truststore.ts")
  public final String sslTrustStorePath;

  /**
   * The SSL trust store password
   */
  @Config("coordinator.ssl.truststore.password")
  @Default("unittestonly")
  public final String sslTrustStorePassword;

  /**
   * The SSL supported cipher suits
   */
  @Config("coordinator.ssl.cipher.suits")
  @Default("TLS_RSA_WITH_AES_128_CBC_SHA256")
  public final String sslCipherSuits;

  public CoordinatorConfig(VerifiableProperties verifiableProperties) {
    this.hostname = verifiableProperties.getString("coordinator.hostname");
    this.datacenterName = verifiableProperties.getString("coordinator.datacenter.name");
    this.requesterPoolSize =
        verifiableProperties.getIntInRange("coordinator.requester.pool.size", 100, 1, Integer.MAX_VALUE);
    this.operationTimeoutMs =
        verifiableProperties.getIntInRange("coordinator.operation.timeout.ms", 2000, 1, Integer.MAX_VALUE);
    this.connectionPoolFactory = verifiableProperties.getString("coordinator.connection.pool.factory",
        "com.github.ambry.network.BlockingChannelConnectionPoolFactory");
    this.connectionPoolCheckoutTimeoutMs =
        verifiableProperties.getIntInRange("coordinator.connection.pool.checkout.timeout.ms", 1000, 1, 5000);
    this.crossDCProxyCallEnable = verifiableProperties.getBoolean("coordinator.cross.dc.proxy.call.enable", true);
    this.sslEnabledDatacenters = verifiableProperties.getString("coordinator.ssl.enabled.datacenters", "");
    this.sslProtocol = verifiableProperties.getString("coordinator.ssl.protocol", "TLS");
    this.sslKeyStoreType = verifiableProperties.getString("coordinator.ssl.keystore.type", "JKS");
    this.sslKeyStorePath = verifiableProperties.getString("coordinator.ssl.keystore.path", "selfsigned-keystore.jks");
    this.sslKeyStorePassword = verifiableProperties.getString("coordinator.ssl.keystore.password", "unittestonly");
    this.sslKeyPassword = verifiableProperties.getString("coordinator.ssl.key.password", "unittestonly");
    this.sslTrustStoreType = verifiableProperties.getString("coordinator.ssl.truststore.type", "JKS");
    this.sslTrustStorePath =
        verifiableProperties.getString("coordinator.ssl.truststore.path", "selfsigned-truststore.ts");
    this.sslTrustStorePassword = verifiableProperties.getString("coordinator.ssl.truststore.password", "unittestonly");
    this.sslCipherSuits =
        verifiableProperties.getString("coordinator.ssl.cipher.suits", "TLS_RSA_WITH_AES_128_CBC_SHA256");
  }
}
