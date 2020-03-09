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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.conscrypt.Conscrypt;
import org.junit.Assert;


public class TestSSLUtils {
  private final static String SSL_CONTEXT_PROTOCOL = "TLS";
  private final static String SSL_CONTEXT_PROVIDER = Conscrypt.isAvailable() ? "Conscrypt" : "SunJSSE";
  private final static String TLS_V1_2_PROTOCOL = "TLSv1.2";
  private static final String SSL_V2_HELLO_PROTOCOL = "SSLv2Hello";
  private final static String ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";
  private final static String SSL_CIPHER_SUITES = "TLS_RSA_WITH_AES_128_CBC_SHA";
  private final static String TRUSTSTORE_PASSWORD = "UnitTestTrustStorePassword";
  private final static String CLIENT_AUTHENTICATION = "required";
  private final static String KEYMANAGER_ALGORITHM = "PKIX";
  private final static String TRUSTMANAGER_ALGORITHM = "PKIX";
  private final static String KEYSTORE_TYPE = "JKS";
  private final static String TRUSTSTORE_TYPE = "JKS";

  /**
   * Create a self-signed X.509 Certificate.
   * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
   *
   * @param dn the X.509 Distinguished Name, eg "CN(commonName)=Test, O(organizationName)=Org"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   * @throws java.security.cert.CertificateException thrown if a security error or an IO error ocurred.
   */
  public static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
      throws CertificateException {
    try {
      Security.addProvider(new BouncyCastleProvider());
      AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
      AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
      AsymmetricKeyParameter privateKeyAsymKeyParam = PrivateKeyFactory.createKey(pair.getPrivate().getEncoded());
      SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
      ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId).build(privateKeyAsymKeyParam);
      X500Name name = new X500Name(dn);
      Date from = new Date();
      Date to = new Date(from.getTime() + days * 86400000L);
      BigInteger sn = new BigInteger(64, new SecureRandom());

      X509v1CertificateBuilder v1CertGen = new X509v1CertificateBuilder(name, sn, from, to, name, subPubKeyInfo);
      X509CertificateHolder certificateHolder = v1CertGen.build(sigGen);
      return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder);
    } catch (CertificateException ce) {
      throw ce;
    } catch (Exception e) {
      throw new CertificateException(e);
    }
  }

  public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, String filename, String password)
      throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(filename);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  /**
   * Creates a keystore with a single key and saves it to a file.
   *
   * @param filename String file to save
   * @param password String store password to set on keystore
   * @param keyPassword String key password to set on key
   * @param alias String alias to use for the key
   * @param privateKey Key to save in keystore
   * @param cert Certificate to use as certificate chain associated to key
   * @throws GeneralSecurityException for any error with the security APIs
   * @throws IOException if there is an I/O error saving the file
   */
  public static void createKeyStore(String filename, String password, String keyPassword, String alias, Key privateKey,
      Certificate cert) throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(), new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  public static <T extends Certificate> void createTrustStore(String filename, String password, Map<String, T> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    try {
      FileInputStream in = new FileInputStream(filename);
      ks.load(in, password.toCharArray());
      in.close();
    } catch (EOFException e) {
      ks = createEmptyKeyStore();
    }
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, filename, password);
  }

  /**
   * Generate a cert and add SSL related properties to {@code props}. This will use {@link #SSL_CONTEXT_PROVIDER} as the
   * provider for SSL routines/libraries.
   * @param props the {@link Properties} instance.
   * @param sslEnabledDatacenters a comma separated list of datacenters where SSL should be enabled.
   * @param mode whether to generate the "client" or "server" certificate.
   * @param trustStoreFile the file path at which to create the trust store.
   * @param certAlias the cert alias to use.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static void addSSLProperties(Properties props, String sslEnabledDatacenters, SSLFactory.Mode mode,
      File trustStoreFile, String certAlias) throws IOException, GeneralSecurityException {
    addSSLProperties(props, sslEnabledDatacenters, mode, trustStoreFile, certAlias, SSL_CONTEXT_PROVIDER);
  }

  /**
   * Setup HTTP2 server related properties. The method should be called after
   * {@link TestSSLUtils#addSSLProperties(Properties, String, SSLFactory.Mode, File, String)}
   * @param properties the {@link Properties} instance.
   * @param mode whether to generate the "client" or "server" certificate.
   * @param sslHttp2SelfSign whether cert is self signed or not.
   */
  public static void addHttp2Properties(Properties properties, SSLFactory.Mode mode, boolean sslHttp2SelfSign) {

    if (mode == SSLFactory.Mode.SERVER) {
      properties.setProperty("rest.server.rest.request.service.factory",
          "com.github.ambry.server.StorageRestRequestService");
      properties.setProperty("rest.server.nio.server.factory", "com.github.ambry.rest.StorageServerNettyFactory");
      properties.setProperty("ssl.client.authentication", "required");
      properties.setProperty("server.enable.store.data.prefetch", "true");
    }
    properties.setProperty("ssl.http2.self.sign", Boolean.toString(sslHttp2SelfSign));
  }

  /**
   * Generate a cert and add SSL related properties to {@code props}
   * @param props the {@link Properties} instance.
   * @param sslEnabledDatacenters a comma separated list of datacenters where SSL should be enabled.
   * @param mode whether to generate the "client" or "server" certificate.
   * @param trustStoreFile the file path at which to create the trust store.
   * @param certAlias the cert alias to use.
   * @param sslContextProvider the name of a registered security provider to use for instantiating {@link SSLContext}.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static void addSSLProperties(Properties props, String sslEnabledDatacenters, SSLFactory.Mode mode,
      File trustStoreFile, String certAlias, String sslContextProvider) throws IOException, GeneralSecurityException {
    Map<String, X509Certificate> certs = new HashMap<>();
    File keyStoreFile;
    String password;

    if (mode == SSLFactory.Mode.CLIENT) {
      password = "UnitTestClientKeyStorePassword";
      keyStoreFile = File.createTempFile("selfsigned-keystore-client", ".jks");
      KeyPair cKP = generateKeyPair("RSA");
      X509Certificate cCert = generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA");
      createKeyStore(keyStoreFile.getPath(), password, password, certAlias, cKP.getPrivate(), cCert);
      certs.put(certAlias, cCert);
    } else {
      password = "UnitTestServerKeyStorePassword";
      keyStoreFile = File.createTempFile("selfsigned-keystore-server", ".jks");
      KeyPair sKP = generateKeyPair("RSA");
      X509Certificate sCert = generateCertificate("CN=localhost, O=server", sKP, 30, "SHA1withRSA");
      createKeyStore(keyStoreFile.getPath(), password, password, certAlias, sKP.getPrivate(), sCert);
      certs.put(certAlias, sCert);
    }

    createTrustStore(trustStoreFile.getPath(), TRUSTSTORE_PASSWORD, certs);

    props.put("ssl.context.protocol", SSL_CONTEXT_PROTOCOL);
    props.put("ssl.context.provider", sslContextProvider == null ? SSL_CONTEXT_PROVIDER : sslContextProvider);
    props.put("ssl.enabled.protocols", TLS_V1_2_PROTOCOL);
    props.put("ssl.endpoint.identification.algorithm", ENDPOINT_IDENTIFICATION_ALGORITHM);
    props.put("ssl.client.authentication", CLIENT_AUTHENTICATION);
    props.put("ssl.keymanager.algorithm", KEYMANAGER_ALGORITHM);
    props.put("ssl.trustmanager.algorithm", TRUSTMANAGER_ALGORITHM);
    props.put("ssl.keystore.type", KEYSTORE_TYPE);
    props.put("ssl.keystore.path", keyStoreFile.getPath());
    props.put("ssl.keystore.password", password);
    props.put("ssl.key.password", password);
    props.put("ssl.truststore.type", TRUSTSTORE_TYPE);
    props.put("ssl.truststore.path", trustStoreFile.getPath());
    props.put("ssl.truststore.password", TRUSTSTORE_PASSWORD);
    props.put("ssl.cipher.suites", SSL_CIPHER_SUITES);
    props.put("clustermap.ssl.enabled.datacenters", sslEnabledDatacenters);
  }

  /**
   * Creates VerifiableProperties with SSL related configs based on the values passed and few other
   * pre-populated values
   * @param sslEnabledDatacenters Comma separated list of datacenters against which ssl connections should be
   *                              established
   * @param mode Represents if the caller is a client or server
   * @param trustStoreFile File path of the truststore file
   * @param certAlias alias used for the certificate
   * @return {@link VerifiableProperties} with all the required values populated
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static VerifiableProperties createSslProps(String sslEnabledDatacenters, SSLFactory.Mode mode,
      File trustStoreFile, String certAlias) throws IOException, GeneralSecurityException {
    Properties props = new Properties();
    addSSLProperties(props, sslEnabledDatacenters, mode, trustStoreFile, certAlias, SSL_CONTEXT_PROVIDER);
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    return new VerifiableProperties(props);
  }

  /**
   * Creates VerifiableProperties with HTTP2 related configs based on the values passed and few other
   * pre-populated values
   * @param sslEnabledDatacenters Comma separated list of datacenters against which ssl connections should be
   *                              established
   * @param mode Represents if the caller is a client or server
   * @param trustStoreFile File path of the truststore file
   * @param certAlias alias used for the certificate
   * @return {@link VerifiableProperties} with all the required values populated
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static VerifiableProperties createHttp2Props(String sslEnabledDatacenters, SSLFactory.Mode mode,
      File trustStoreFile, String certAlias) throws IOException, GeneralSecurityException {
    Properties props = new Properties();
    addSSLProperties(props, sslEnabledDatacenters, mode, trustStoreFile, certAlias, SSL_CONTEXT_PROVIDER);
    addHttp2Properties(props, mode, false);
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    return new VerifiableProperties(props);
  }

  private static void verifySSLConfig(SSLContext sslContext, SSLEngine sslEngine, boolean isClient) {
    // SSLContext verify
    Assert.assertEquals(sslContext.getProtocol(), SSL_CONTEXT_PROTOCOL);
    Assert.assertEquals(sslContext.getProvider().getName(), SSL_CONTEXT_PROVIDER);

    // SSLEngine verify
    String[] enabledProtocols = sslEngine.getEnabledProtocols();
    if (enabledProtocols.length == 2) {
      // Apparently the Netty OpenSslEngine has no way of disabling the SSLv2Hello protocol.
      // This is the relevant code from ReferenceCountedOpenSslEngine.getEnabledProtocols():
      // """
      // // Seems like there is no way to explicit disable SSLv2Hello in openssl so it is always enabled
      // enabled.add(PROTOCOL_SSL_V2_HELLO);
      // """
      Assert.assertArrayEquals("enabledProtocols does not match expected",
          new String[]{SSL_V2_HELLO_PROTOCOL, TLS_V1_2_PROTOCOL}, enabledProtocols);
    } else {
      Assert.assertArrayEquals("enabledProtocols does not match expected", new String[]{TLS_V1_2_PROTOCOL},
          enabledProtocols);
    }
    String[] enabledCipherSuite = sslEngine.getEnabledCipherSuites();
    Assert.assertEquals(enabledCipherSuite.length, 1);
    Assert.assertEquals(enabledCipherSuite[0], SSL_CIPHER_SUITES);
    Assert.assertEquals(sslEngine.getWantClientAuth(), false);
    if (isClient) {
      Assert.assertEquals(sslEngine.getSSLParameters().getEndpointIdentificationAlgorithm(),
          ENDPOINT_IDENTIFICATION_ALGORITHM);
      Assert.assertEquals(sslEngine.getNeedClientAuth(), false);
      Assert.assertEquals(sslEngine.getUseClientMode(), true);
    } else {
      Assert.assertEquals(sslEngine.getSSLParameters().getEndpointIdentificationAlgorithm(), null);
      Assert.assertEquals(sslEngine.getNeedClientAuth(), true);
      Assert.assertEquals(sslEngine.getUseClientMode(), false);
    }
  }

  /**
   * Test instantiating an implementation of {@link SSLFactory} using reflection and verify the {@link SSLEngine}
   * configuration.
   * @param factoryClassName the full class name for the {@link SSLFactory} to instantiate.
   * @throws Exception
   */
  public static void testSSLFactoryImpl(String factoryClassName) throws Exception {
    //server
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig serverSslConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    SSLFactory sslFactory = Utils.getObj(factoryClassName, serverSslConfig);
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();
    Assert.assertNotNull(socketFactory);
    SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
    Assert.assertNotNull(serverSocketFactory);
    SSLEngine serverSideSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);
    TestSSLUtils.verifySSLConfig(sslContext, serverSideSSLEngine, false);

    //client
    SSLConfig clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));
    sslFactory = Utils.getObj(factoryClassName, clientSSLConfig);
    sslContext = sslFactory.getSSLContext();
    socketFactory = sslContext.getSocketFactory();
    Assert.assertNotNull(socketFactory);
    serverSocketFactory = sslContext.getServerSocketFactory();
    Assert.assertNotNull(serverSocketFactory);
    SSLEngine clientSideSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
    TestSSLUtils.verifySSLConfig(sslContext, clientSideSSLEngine, true);
  }
}
