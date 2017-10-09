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
package com.github.ambry.router;

import com.github.ambry.utils.TestUtils;
import java.util.Properties;


/**
 * Utilities used for KMS and CryptoService tests
 */
class CryptoTestUtils {

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link SingleKeyManagementService}.
   * @param key the single default key to be set
   * @param randomKeySizeInBits random key size in bits value
   * @return the created Properties instance.
   */
  static Properties getKMSProperties(String key, int randomKeySizeInBits) {
    Properties properties = new Properties();
    properties.setProperty("kms.default.container.key", key);
    properties.setProperty("kms.random.key.size.in.bits", Integer.toString(randomKeySizeInBits));
    properties.setProperty("clustermap.cluster.name", "dev");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    return properties;
  }

  /**
   * Generates and returns a random Hex String of the specified size
   * @param size expected key hex string size
   * @return the hex string thus generated
   */
  static String getRandomKey(int size) {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < size) {
      sb.append(Integer.toHexString(TestUtils.RANDOM.nextInt()));
    }
    sb.setLength(size);
    return sb.toString();
  }
}
