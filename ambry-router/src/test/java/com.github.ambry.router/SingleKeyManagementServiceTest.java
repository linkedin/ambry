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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.security.GeneralSecurityException;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.router.CryptoTestUtils.*;


/**
 * Tests {@link SingleKeyManagementService} and {@link SingleKeyManagementServiceFactory}
 */
public class SingleKeyManagementServiceTest {

  static final int DEFAULT_KEY_SIZE_CHARS = 64;
  private static final int DEFAULT_RANDOM_KEY_SIZE_BITS = 256;
  private static final String CLUSTER_NAME = TestUtils.getRandomString(10);
  private static final MetricRegistry REGISTRY = new MetricRegistry();

  /**
   * Test {@link SingleKeyManagementService#getKey(short, short)}
   */
  @Test
  public void testSingleKMS() throws Exception {
    int[] keySizes = {16, 32, 64, 128};
    for (int keySize : keySizes) {
      String key = TestUtils.getRandomKey(keySize);
      Properties props = getKMSProperties(key, DEFAULT_RANDOM_KEY_SIZE_BITS);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      KMSConfig config = new KMSConfig(verifiableProperties);
      SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), config.kmsKeyGenAlgo);
      KeyManagementService<SecretKeySpec> kms =
          new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
      SecretKeySpec keyFromKMS =
          kms.getKey(Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM));
      Assert.assertEquals("Secret key mismatch ", secretKeySpec, keyFromKMS);
    }
  }

  /**
   * Tests {@link SingleKeyManagementService#getRandomKey()}
   */
  @Test
  public void testRandomKey() throws Exception {
    int[] keySizes = {128, 192, 256};
    for (int keySize : keySizes) {
      String key = TestUtils.getRandomKey(keySize);
      Properties props = getKMSProperties(key, keySize);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      KeyManagementService<SecretKeySpec> kms =
          new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
      SecretKeySpec randomKey = kms.getRandomKey();
      Assert.assertNotNull("Random key cannot be null", randomKey);
      Assert.assertEquals("Key size mismatch", keySize, randomKey.getEncoded().length * 8);
    }
  }

  /**
   * Test the {@link SingleKeyManagementService} for close()
   */
  @Test
  public void testSingleKMSClose() throws Exception {
    String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_CHARS);
    Properties props = getKMSProperties(key, DEFAULT_RANDOM_KEY_SIZE_BITS);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> kms =
        new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
    kms.close();
    try {
      kms.getKey(Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID);
      Assert.fail("getKey() on KMS should have failed as KMS is closed");
    } catch (GeneralSecurityException e) {
    }
  }

  /**
   * Tests {@link SingleKeyManagementServiceFactory}
   */
  @Test
  public void testSingleKMSFactory() throws Exception {
    Properties props = getKMSProperties("", DEFAULT_KEY_SIZE_CHARS);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
      Assert.fail("SingleKeyManagementFactory instantiation should have failed as single default key is null ");
    } catch (IllegalArgumentException e) {
    }

    // happy path
    String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_CHARS);
    props = getKMSProperties(key, DEFAULT_RANDOM_KEY_SIZE_BITS);
    verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> kms =
        new SingleKeyManagementServiceFactory(verifiableProperties, CLUSTER_NAME, REGISTRY).getKeyManagementService();
  }
}
