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
import javax.net.ssl.SSLEngine;
import org.junit.Test;


/**
 * Test {@link NettySslFactory}
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
}
