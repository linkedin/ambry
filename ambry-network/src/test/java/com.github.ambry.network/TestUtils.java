package com.github.ambry.network;

import java.util.ArrayList;


public class TestUtils {
  public static SSLFactory createSSLFactory()
      throws Exception {
    String keyStoreFileName = "./ambry-network/src/test/java/com.github.ambry.network/selfsigned-keystore.jks";
    String trustStoreFileName = "./ambry-network/src/test/java/com.github.ambry.network/selfsigned-truststore.ts";
    SSLFactory sslFactory = new SSLFactory();
    sslFactory.setProtocol("TLS");
    sslFactory.setKeyStore("JKS", keyStoreFileName, "unittestonly", "unittestonly");
    sslFactory.setTrustStore("JKS", trustStoreFileName, "unittestonly");
    sslFactory.setProvider("SunJSSE");
    ArrayList<String> supportedCipherSuites = new ArrayList<String>();
    supportedCipherSuites.add("TLS_RSA_WITH_AES_128_CBC_SHA256");
    sslFactory.setCipherSuites(supportedCipherSuites);
    ArrayList<String> supportedProtocols = new ArrayList<String>();
    supportedProtocols.add("TLSv1.2");
    sslFactory.setEnabledProtocols(supportedProtocols);
    return sslFactory;
  }
}
