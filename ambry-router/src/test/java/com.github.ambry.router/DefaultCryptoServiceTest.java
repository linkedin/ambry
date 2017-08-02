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

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.router.CryptoUtils.*;


/**
 * Tests {@link DefaultCryptoService} and {@link DefaultCryptoServiceFactory}
 */
public class DefaultCryptoServiceTest {

  /**
   * Tests basic encryption and decryption for different sizes of keys and random data
   */
  @Test
  public void testDefaultCryptoService() throws Exception {
    for (int j = 0; j < 3; j++) {
      String key = getRandomKey(64);
      Properties props = getKMSProperties(key);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
      CryptoService<SecretKeySpec> cryptoService =
          new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService(secretKeySpec);
      for (int i = 0; i < 5; i++) {
        int size = TestUtils.RANDOM.nextInt(100000);
        byte[] randomData = new byte[size];
        TestUtils.RANDOM.nextBytes(randomData);
        byte[] encryptedBytes = cryptoService.encrypt(randomData);
        byte[] decryptedBytes = cryptoService.decrypt(encryptedBytes);
        Assert.assertArrayEquals("Decrypted bytes and plain bytes should match", randomData, decryptedBytes);
      }
    }
  }

  /**
   * Tests failure scenarios for decryption
   * @throws InstantiationException
   */
  @Test
  public void testDecryptionFailure() throws InstantiationException {
    String key = getRandomKey(64);
    Properties props = getKMSProperties(key);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
    CryptoService<SecretKeySpec> cryptoService =
        new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService(secretKeySpec);
    int size = TestUtils.RANDOM.nextInt(100000);
    byte[] randomData = new byte[size];
    TestUtils.RANDOM.nextBytes(randomData);
    try {
      cryptoService.decrypt(randomData);
      Assert.fail("Decryption should have failed as input data is not encrypted");
    } catch (CryptoServiceException e) {
    }
  }

  /**
   * Test the {@link DefaultKeyManagementServiceFactory}
   */
  @Test
  public void testDefaultKeyManagementServiceFactory() throws Exception {
    // happy path
    String key = getRandomKey(64);
    Properties props = getKMSProperties(key);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService(new SecretKeySpec(Hex.decode(key), "AES"));
  }
}
