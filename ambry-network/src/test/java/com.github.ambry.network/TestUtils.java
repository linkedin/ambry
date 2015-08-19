package com.github.ambry.network;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;


public class TestUtils {
  public static SSLFactory createSSLFactory()
      throws Exception {
    ClassLoader cl = TestUtils.class.getClassLoader();
    URL keyStoreUrl = cl.getResource("selfsigned-keystore.jks");
    String keyStoreFilePath = new File(keyStoreUrl.toURI()).getAbsolutePath();
    URL trustStoreUrl = cl.getResource("selfsigned-truststore.ts");
    String trustStoreFilePath = new File(trustStoreUrl.toURI()).getAbsolutePath();

    SSLFactory sslFactory = new SSLFactory();
    sslFactory.setProtocol("TLS");
    sslFactory.setKeyStore("JKS", keyStoreFilePath, "unittestonly", "unittestonly");
    sslFactory.setTrustStore("JKS", trustStoreFilePath, "unittestonly");
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
