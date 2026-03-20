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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.Properties;
import javax.net.ssl.SSLEngine;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test {@link NettySslHttp2Factory}
 */
public class NettySslHttp2FactoryTest {

  /**
   * Run sanity checks for {@link NettySslHttp2Factory}. Make sure no exception.
   * @throws Exception
   */
  @Test
  public void testHttp2SSLFactory() throws Exception {
    //server
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig serverSslConfig =
        new SSLConfig(TestSSLUtils.createHttp2Props("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    NettySslHttp2Factory sslFactory = Utils.getObj(NettySslHttp2Factory.class.getName(), serverSslConfig);
    SSLEngine ssLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);

    SSLConfig clientSSLConfig =
        new SSLConfig(TestSSLUtils.createHttp2Props("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));
    sslFactory = Utils.getObj(NettySslHttp2Factory.class.getName(), clientSSLConfig);
    ssLEngine = sslFactory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
  }

  /**
   * Verify that default session cache size (20480) and timeout (300s) are applied when not explicitly configured.
   */
  @Test
  public void testDefaultSessionCacheConfig() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig sslConfig =
        new SSLConfig(TestSSLUtils.createHttp2Props("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    Assert.assertEquals(20480, sslConfig.sslSessionCacheSize);
    Assert.assertEquals(300, sslConfig.sslSessionTimeoutSec);
  }

  /**
   * Verify that non-default session cache size and timeout values are applied to the Http2 SslContext.
   * @throws Exception
   */
  @Test
  public void testSessionCacheConfig() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    Properties props = new Properties();
    TestSSLUtils.addSSLProperties(props, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    TestSSLUtils.addHttp2Properties(props, SSLFactory.Mode.SERVER, false);
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("ssl.session.cache.size", "100");
    props.setProperty("ssl.session.timeout.sec", "60");
    SSLConfig sslConfig = new SSLConfig(new VerifiableProperties(props));
    Assert.assertEquals(100, sslConfig.sslSessionCacheSize);
    Assert.assertEquals(60, sslConfig.sslSessionTimeoutSec);

    NettySslHttp2Factory factory = new NettySslHttp2Factory(sslConfig);
    SSLEngine serverEngine = factory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);
    Assert.assertNotNull(serverEngine);
    SSLEngine clientEngine = factory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
    Assert.assertNotNull(clientEngine);
  }
}
