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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.router.CryptoUtils.*;


/**
 * Tests {@link DefaultKeyManagementService} and {@link DefaultKeyManagementServiceFactory}
 */
public class DefaultKeyManagementServiceTest {

  /**
   * Test the {@link DefaultKeyManagementService} for happy getKey() path with FutureResult
   */
  @Test
  public void testDefaultKMSBasic() throws Exception {
    int[] keySizes = {16, 32, 64, 128};
    for (int keySize : keySizes) {
      String key = getRandomKey(keySize);
      Properties props = getKMSProperties(key);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      KMSConfig KMSConfig = new KMSConfig(verifiableProperties);
      SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), KMSConfig.kmsKeyGenAlgo);
      KeyManagementService<SecretKeySpec> defaultKMS =
          new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
      FutureResult<SecretKeySpec> result = defaultKMS.getKey("", Account.UNKNOWN_ACCOUNT, Container.UNKNOWN_CONTAINER);
      SecretKeySpec loadedKey = result.get(1, TimeUnit.SECONDS);
      Assert.assertEquals("Secret key mismatch ", secretKeySpec, loadedKey);
    }
  }

  /**
   * Test the {@link DefaultKeyManagementService} for happy getKey() path with Callback
   */
  @Test
  public void testDefaultKMSCallback() throws Exception {
    int[] keySizes = {16, 32, 64, 128};
    for (int keySize : keySizes) {
      String key = getRandomKey(keySize);
      Properties props = getKMSProperties(key);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      KMSConfig KMSConfig = new KMSConfig(verifiableProperties);
      SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), KMSConfig.kmsKeyGenAlgo);
      KeyManagementService<SecretKeySpec> defaultKMS =
          new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
      defaultKMS.getKey("", Account.UNKNOWN_ACCOUNT, Container.UNKNOWN_CONTAINER, new Callback<SecretKeySpec>() {
        @Override
        public void onCompletion(SecretKeySpec result, Exception exception) {
          Assert.assertEquals("Secret key mismatch ", secretKeySpec, result);
          Assert.assertNull("Exception should be null ", exception);
        }
      });
    }
  }

  /**
   * Test the {@link DefaultKeyManagementService} with different cluster names
   */
  @Test
  public void testDefaultKMSDiffClusterNames() throws Exception {
    String key = getRandomKey(64);
    Properties props = getKMSProperties(key);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    KMSConfig KMSConfig = new KMSConfig(verifiableProperties);
    SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), KMSConfig.kmsKeyGenAlgo);
    KeyManagementService<SecretKeySpec> defaultKMS =
        new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
    String[] clusterNames = {"", "Staging", "Production"};
    for (String clusterName : clusterNames) {
      // for any clusterName, same key is expected
      FutureResult<SecretKeySpec> result =
          defaultKMS.getKey(clusterName, Account.UNKNOWN_ACCOUNT, Container.UNKNOWN_CONTAINER);
      SecretKeySpec loadedKey = result.get(1, TimeUnit.SECONDS);
      Assert.assertEquals("Secret key mismatch ", secretKeySpec, loadedKey);
    }
  }

  /**
   * Test the {@link DefaultKeyManagementService} for close()
   */
  @Test
  public void testDefaultKMSClose() throws Exception {
    String key = getRandomKey(64);
    Properties props = getKMSProperties(key);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> defaultKMS =
        new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
    defaultKMS.close();
    try {
      defaultKMS.getKey("", Account.UNKNOWN_ACCOUNT, Container.UNKNOWN_CONTAINER);
      Assert.fail("getKey() on DefaultKMS should have failed as KMS is closed");
    } catch (IllegalStateException e) {
    }
  }

  /**
   * Test the {@link DefaultKeyManagementServiceFactory}
   */
  @Test
  public void testDefaultKMSFactory() throws Exception {
    Properties props = getKMSProperties("");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
      Assert.fail("DefaultKeyManagementFactory instantiation should have failed as key store path is null ");
    } catch (IllegalArgumentException e) {
    }

    // happy path
    String key = getRandomKey(64);
    props = getKMSProperties(key);
    verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> defaultKMS =
        new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
  }
}
