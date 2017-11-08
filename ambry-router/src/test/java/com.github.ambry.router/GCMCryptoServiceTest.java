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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.router.CryptoTestUtils.*;


/**
 * Tests {@link GCMCryptoService} and {@link GCMCryptoServiceFactory}
 */
public class GCMCryptoServiceTest {

  private static final int MAX_DATA_SIZE = 10000;
  private static final int DEFAULT_KEY_SIZE_IN_CHARS = 64;
  private static final MetricRegistry REGISTRY = new MetricRegistry();

  /**
   * Tests basic encryption and decryption for different sizes of keys and random data in bytes
   */
  @Test
  public void testEncryptDecryptBytes() throws Exception {
    for (int j = 0; j < 3; j++) {
      String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
      Properties props = getKMSProperties(key, DEFAULT_KEY_SIZE_IN_CHARS);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
      CryptoService<SecretKeySpec> cryptoService =
          new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
      for (int i = 0; i < 5; i++) {
        int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE);
        byte[] randomData = new byte[size];
        TestUtils.RANDOM.nextBytes(randomData);
        ByteBuffer toEncrypt = ByteBuffer.wrap(randomData);
        ByteBuffer encryptedBytes = cryptoService.encrypt(toEncrypt, secretKeySpec);
        Assert.assertFalse("Encrypted bytes and plain bytes should not match",
            Arrays.equals(randomData, encryptedBytes.array()));
        ByteBuffer decryptedBytes = cryptoService.decrypt(encryptedBytes, secretKeySpec);
        Assert.assertArrayEquals("Decrypted bytes and plain bytes should match", randomData, decryptedBytes.array());
      }
    }
  }

  /**
   * Tests encryption and decryption of keys with random data
   */
  @Test
  public void testEncryptDecryptKeys() throws Exception {
    for (int j = 0; j < 5; j++) {
      String keyToEncrypt = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
      Properties props = getKMSProperties(keyToEncrypt, DEFAULT_KEY_SIZE_IN_CHARS);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      SecretKeySpec secretKeyToEncrypt = new SecretKeySpec(Hex.decode(keyToEncrypt), "AES");
      String keyToBeEncrypted = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
      SecretKeySpec secretKeyToBeEncrypted = new SecretKeySpec(Hex.decode(keyToBeEncrypted), "AES");
      CryptoService<SecretKeySpec> cryptoService =
          new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
      ByteBuffer encryptedBytes = cryptoService.encryptKey(secretKeyToBeEncrypted, secretKeyToEncrypt);
      Assert.assertFalse("Encrypted key and plain key should not match",
          Arrays.equals(keyToBeEncrypted.getBytes(), encryptedBytes.array()));
      SecretKeySpec decryptedKey = cryptoService.decryptKey(encryptedBytes, secretKeyToEncrypt);
      Assert.assertEquals("Decrypted Key and original key mismatch", secretKeyToBeEncrypted, decryptedKey);
    }
  }

  /**
   * Tests encryption and decryption of keys with diff iv sizes
   */
  @Test
  public void testEncryptDecryptKeysDiffIvSize() throws Exception {
    int[] ivSizes = {12, 16, 24, 32, 48, 64};
    for (int ivSize : ivSizes) {
      String keyToEncrypt = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
      Properties props = getKMSProperties(keyToEncrypt, DEFAULT_KEY_SIZE_IN_CHARS);
      props.setProperty("crypto.service.iv.size.in.bytes", Integer.toString(ivSize));
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      SecretKeySpec secretKeyToEncrypt = new SecretKeySpec(Hex.decode(keyToEncrypt), "AES");
      String keyToBeEncrypted = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
      SecretKeySpec secretKeyToBeEncrypted = new SecretKeySpec(Hex.decode(keyToBeEncrypted), "AES");
      CryptoService<SecretKeySpec> cryptoService =
          new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
      ByteBuffer encryptedBytes = cryptoService.encryptKey(secretKeyToBeEncrypted, secretKeyToEncrypt);
      Assert.assertFalse("Encrypted key and plain key should not match",
          Arrays.equals(keyToBeEncrypted.getBytes(), encryptedBytes.array()));
      SecretKeySpec decryptedKey = cryptoService.decryptKey(encryptedBytes, secretKeyToEncrypt);
      Assert.assertEquals("Decrypted Key and original key mismatch", secretKeyToBeEncrypted, decryptedKey);
    }
  }

  /**
   * Tests failure scenarios for decryption
   * @throws InstantiationException
   */
  @Test
  public void testDecryptionFailure() throws InstantiationException {
    String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
    Properties props = getKMSProperties(key, DEFAULT_KEY_SIZE_IN_CHARS);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
    CryptoService<SecretKeySpec> cryptoService =
        new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
    int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE);
    byte[] randomData = new byte[size];
    TestUtils.RANDOM.nextBytes(randomData);
    ByteBuffer toDecrypt = ByteBuffer.wrap(randomData);
    try {
      cryptoService.decrypt(toDecrypt, secretKeySpec);
      Assert.fail("Decryption should have failed as input data is not encrypted");
    } catch (GeneralSecurityException e) {
    }

    try {
      cryptoService.decryptKey(toDecrypt, secretKeySpec);
      Assert.fail("Decryption of key should have failed as input data is not encrypted");
    } catch (GeneralSecurityException e) {
    }
  }

  /**
   * Test {@link GCMCryptoServiceFactory}
   */
  @Test
  public void testGMCryptoServiceFactory() throws Exception {
    // happy path
    VerifiableProperties verifiableProperties = new VerifiableProperties((new Properties()));
    new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();

    // unrecognized mode
    String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
    Properties props = getKMSProperties(key, DEFAULT_KEY_SIZE_IN_CHARS);
    props.setProperty("crypto.service.encryption.decryption.mode", "CBC");
    verifiableProperties = new VerifiableProperties((props));
    try {
      CryptoService<SecretKeySpec> cryptoService =
          new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
      Assert.fail("IllegalArgumentException should have thrown for un-recognized mode ");
    } catch (IllegalArgumentException e) {
    }
  }
}
