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
import java.io.File;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class SSLFactoryTest {

  @Before
  public void setup() throws Exception {
  }

  @After
  public void teardown() throws Exception {
  }

  @Test
  public void testSSLFactory() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig sslConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    SSLConfig clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));

    SSLFactory sslFactory = new SSLFactory(sslConfig);
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLSocketFactory socketFactory = sslContext.getSocketFactory();
    Assert.assertNotNull(socketFactory);
    SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
    Assert.assertNotNull(serverSocketFactory);
    SSLEngine serverSideSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);
    TestSSLUtils.verifySSLConfig(sslContext, serverSideSSLEngine, false);

    //client
    sslFactory = new SSLFactory(clientSSLConfig);
    sslContext = sslFactory.getSSLContext();
    socketFactory = sslContext.getSocketFactory();
    Assert.assertNotNull(socketFactory);
    serverSocketFactory = sslContext.getServerSocketFactory();
    Assert.assertNotNull(serverSocketFactory);
    SSLEngine clientSideSSLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
    TestSSLUtils.verifySSLConfig(sslContext, clientSideSSLEngine, true);
  }
}
