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
import java.io.File;
import java.util.Properties;
import javax.net.ssl.SSLEngine;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test {@link NettySslFactory}
 */
public class NettySslFactoryTest {

  /**
   * Run sanity checks for {@link NettySslFactory}.
   * @throws Exception
   */
  @Test
  public void testSSLFactory() throws Exception {
    TestSSLUtils.testSSLFactoryImpl(NettySslFactory.class.getName());
  }

  /**
   * Verify that non-default session cache size and timeout values are applied to the SslContext.
   * @throws Exception
   */
  @Test
  public void testSessionCacheConfig() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    Properties props = new Properties();
    TestSSLUtils.addSSLProperties(props, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("ssl.session.cache.size", "100");
    props.setProperty("ssl.session.timeout.sec", "60");
    SSLConfig sslConfig = new SSLConfig(new com.github.ambry.config.VerifiableProperties(props));
    Assert.assertEquals(100, sslConfig.sslSessionCacheSize);
    Assert.assertEquals(60, sslConfig.sslSessionTimeoutSec);

    NettySslFactory factory = new NettySslFactory(sslConfig);
    SSLEngine serverEngine = factory.createSSLEngine("localhost", 9095, SSLFactory.Mode.SERVER);
    Assert.assertNotNull(serverEngine);
    SSLEngine clientEngine = factory.createSSLEngine("localhost", 9095, SSLFactory.Mode.CLIENT);
    Assert.assertNotNull(clientEngine);
  }
}
