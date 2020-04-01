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
import com.github.ambry.utils.TestUtils;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link JdkSslFactory}
 */
public class JdkSslFactoryTest {

  /**
   * Run sanity checks for {@link JdkSslFactory}.
   * @throws Exception
   */
  @Test
  public void testSSLFactory() throws Exception {
    TestSSLUtils.testSSLFactoryImpl(JdkSslFactory.class.getName());

    // test features specific to JDK impls, like the PRNG algorithm config
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    Properties props = new Properties();
    TestSSLUtils.addSSLProperties(props, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
    for (String prngAlgorithm : new String[]{"NativePRNGNonBlocking", "SHA1PRNG", "Windows-PRNG", "badbadinvalid"}) {
      // First check if the algorithm is supported by the system/jdk/security provider.
      boolean valid = true;
      try {
        SecureRandom.getInstance(prngAlgorithm);
      } catch (NoSuchAlgorithmException e) {
        valid = false;
      }
      props.put("ssl.secure.random.algorithm", prngAlgorithm);
      SSLConfig config = new SSLConfig(new VerifiableProperties(props));
      if (valid) {
        JdkSslFactory jdkSslFactory = new JdkSslFactory(config);
        assertNotNull("Invalid SSLContext", jdkSslFactory.getSSLContext());
      } else {
        TestUtils.assertException(NoSuchAlgorithmException.class, () -> new JdkSslFactory(config), null);
      }
    }
    // leaving this prop empty should use the default impl.
    props.put("ssl.secure.random.algorithm", "");
    SSLConfig config = new SSLConfig(new VerifiableProperties(props));
    JdkSslFactory jdkSslFactory = new JdkSslFactory(config);
    assertNotNull("Invalid SSLContext", jdkSslFactory.getSSLContext());
  }
}
