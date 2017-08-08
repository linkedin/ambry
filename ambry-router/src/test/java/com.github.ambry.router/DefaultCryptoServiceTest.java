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
import java.nio.ByteBuffer;
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

  private static final int maxDataSize = 10000;

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
          new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService();
      for (int i = 0; i < 5; i++) {
        int size = TestUtils.RANDOM.nextInt(maxDataSize);
        byte[] randomData = new byte[size];
        TestUtils.RANDOM.nextBytes(randomData);
        ByteBuffer toEncrypt = ByteBuffer.wrap(randomData);
        ByteBuffer encryptedBytes = cryptoService.encrypt(toEncrypt, secretKeySpec);
        ByteBuffer decryptedBytes = cryptoService.decrypt(encryptedBytes, secretKeySpec);
        Assert.assertArrayEquals("Decrypted bytes and plain bytes should match", randomData, decryptedBytes.array());
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
        new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService();
    int size = TestUtils.RANDOM.nextInt(maxDataSize);
    byte[] randomData = new byte[size];
    TestUtils.RANDOM.nextBytes(randomData);
    ByteBuffer toDecrypt = ByteBuffer.wrap(randomData);
    try {
      cryptoService.decrypt(toDecrypt, secretKeySpec);
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
    VerifiableProperties verifiableProperties = new VerifiableProperties((new Properties()));
    new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService();

    // unrecognized mode
    String key = getRandomKey(64);
    Properties props = getKMSProperties(key);
    props.setProperty("crypto.service.encryption.decryption.mode", "CBC");
    verifiableProperties = new VerifiableProperties((props));
    try {
      CryptoService<SecretKeySpec> cryptoService =
          new DefaultCryptoServiceFactory(verifiableProperties).getCryptoService();
      Assert.fail("IllegalArgumentException should have thrown for un-recognized mode ");
    } catch (IllegalArgumentException e) {
    }
  }
}
