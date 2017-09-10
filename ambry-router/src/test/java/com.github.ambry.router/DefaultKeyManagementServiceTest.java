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
 * Tests {@link DefaultKeyManagementService} and {@link DefaultKeyManagementServiceFactory}
 */
public class DefaultKeyManagementServiceTest {

  private static final int defaultKeySize = 64;

  /**
   * Test the {@link DefaultKeyManagementService} for happy getKey() path
   */
  @Test
  public void testDefaultKMS() throws Exception {
    int[] keySizes = {16, 32, 64, 128};
    for (int keySize : keySizes) {
      String key = getRandomKey(keySize);
      Properties props = getKMSProperties(key, keySize);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      KMSConfig KMSConfig = new KMSConfig(verifiableProperties);
      SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), KMSConfig.kmsKeyGenAlgo);
      KeyManagementService<SecretKeySpec> defaultKMS =
          new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
      SecretKeySpec keyFromKMS =
          defaultKMS.getKey(Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM));
      Assert.assertEquals("Secret key mismatch ", secretKeySpec, keyFromKMS);
    }
  }

  /**
   * Test the {@link DefaultKeyManagementService} for {@link DefaultKeyManagementService#register(short, short)}
   */
  @Test
  public void testDefaultKMSRegister() throws Exception {
    String key = getRandomKey(defaultKeySize);
    Properties props = getKMSProperties(key, defaultKeySize);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> defaultKMS =
        new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
    defaultKMS.register(Account.UNKNOWN_ACCOUNT.getId(), Container.UNKNOWN_CONTAINER.getId());
  }

  /**
   * Test the {@link DefaultKeyManagementService} for close()
   */
  @Test
  public void testDefaultKMSClose() throws Exception {
    String key = getRandomKey(defaultKeySize);
    Properties props = getKMSProperties(key, defaultKeySize);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> defaultKMS =
        new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
    defaultKMS.close();
    try {
      defaultKMS.getKey(Account.UNKNOWN_ACCOUNT.getId(), Container.UNKNOWN_CONTAINER.getId());
      Assert.fail("getKey() on DefaultKMS should have failed as KMS is closed");
    } catch (GeneralSecurityException e) {
    }
  }

  /**
   * Test the {@link DefaultKeyManagementServiceFactory}
   */
  @Test
  public void testDefaultKMSFactory() throws Exception {
    Properties props = getKMSProperties("", defaultKeySize);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
      Assert.fail("DefaultKeyManagementFactory instantiation should have failed as key store path is null ");
    } catch (IllegalArgumentException e) {
    }

    // happy path
    String key = getRandomKey(defaultKeySize);
    props = getKMSProperties(key, defaultKeySize);
    verifiableProperties = new VerifiableProperties((props));
    KeyManagementService<SecretKeySpec> defaultKMS =
        new DefaultKeyManagementServiceFactory(verifiableProperties).getKeyManagementService();
  }
}
