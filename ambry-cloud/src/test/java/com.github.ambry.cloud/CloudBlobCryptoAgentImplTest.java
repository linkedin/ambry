/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.CryptoService;
import com.github.ambry.router.GCMCryptoService;
import com.github.ambry.router.KeyManagementService;
import com.github.ambry.router.SingleKeyManagementService;
import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests CloudBlobCryptoAgentImpl
 */
public class CloudBlobCryptoAgentImplTest {

  private CloudBlobCryptoAgentImpl cryptoAgent;
  private static final int TWO_FIFTY_SIX_BITS_IN_BYTES = 32;

  public CloudBlobCryptoAgentImplTest() throws GeneralSecurityException {
    CryptoService cryptoService =
        new GCMCryptoService(new CryptoServiceConfig(new VerifiableProperties(new Properties())));
    KeyManagementService kms = new SingleKeyManagementService(new KMSConfig(new VerifiableProperties(new Properties())),
        TestUtils.getRandomKey(TWO_FIFTY_SIX_BITS_IN_BYTES));
    cryptoAgent = new CloudBlobCryptoAgentImpl(cryptoService, kms, "any_context");
  }

  /**
   * Tests basic encrypt -> decrypt functionality
   * @throws Exception
   */
  @Test
  public void testBasicFunctionality() throws Exception {
    byte[] payload = TestUtils.getRandomBytes(10000);
    byte[] encryptedPayload = cryptoAgent.encrypt(ByteBuffer.wrap(payload)).array();
    assertFalse(payload.length == encryptedPayload.length);
    assertNotSubset("Payload is a subset of the 'encrypted' payload", encryptedPayload, payload);
    byte[] decryptedPayload = cryptoAgent.decrypt(ByteBuffer.wrap(encryptedPayload)).array();
    assertNotSame("Payload and decrypted payload are the same array instance", payload, decryptedPayload);
    assertArrayEquals("Decrypted payload different from original payload", payload, decryptedPayload);
  }

  /**
   * Tests that all data validation checks work for decryption
   * @throws GeneralSecurityException
   */
  @Test
  public void testBadDecryptInput() throws GeneralSecurityException {
    byte[] garbage = TestUtils.getRandomBytes(10000);
    //zero out version, encrypted key size, encrypted data size fields
    for (int i = 0; i < CloudBlobCryptoAgentImpl.EncryptedDataPayload.INITIAL_MESSAGE_LENGTH; i++) {
      garbage[i] = 0;
    }
    //Version check should fail
    try {
      cryptoAgent.decrypt(ByteBuffer.wrap(garbage)).array();
      fail("Should have thrown exception");
    } catch (GeneralSecurityException e) {
      //expected
    }
    //Version check pass, encrypted key size check should fail
    garbage[1] = 1;
    garbage[2] = -128;
    try {
      cryptoAgent.decrypt(ByteBuffer.wrap(garbage)).array();
      fail("Should have thrown exception");
    } catch (GeneralSecurityException e) {
      //expected
    }
    //Version check, encrypted key size check pass, encrypted data size check fail
    garbage[2] = 0;
    garbage[5] = 79;
    garbage[6] = -128;
    garbage[10] = -128;
    try {
      cryptoAgent.decrypt(ByteBuffer.wrap(garbage)).array();
      fail("Should have thrown exception");
    } catch (GeneralSecurityException e) {
      //expected
    }
    //Version check, encrypted key size check, encrypted data size check pass,
    //crc check fail
    garbage[6] = 0;
    garbage[10] = 0;
    garbage[13] = 79;
    try {
      cryptoAgent.decrypt(ByteBuffer.wrap(garbage)).array();
      fail("Should have thrown exception");
    } catch (GeneralSecurityException e) {
      //expected
    }
  }

  /**
   * Throws an exception if the smaller array is a subset of the larger array
   * @param message message sent with exception
   * @param arr0
   * @param arr1
   * @throws Exception
   */
  private void assertNotSubset(String message, byte[] arr0, byte[] arr1) throws Exception {
    boolean found;
    if (arr0 == null || arr1 == null) {
      throw new Exception("Array arguments should not be null");
    }
    if (arr0.length == 0 || arr1.length == 0) {
      throw new Exception("Array arguments should not have 0 length");
    }
    byte[] largerArray = arr0.length > arr1.length ? arr0 : arr1;
    byte[] smallerArray = largerArray == arr0 ? arr1 : arr0;

    for (int i = 0; i <= largerArray.length - smallerArray.length; i++) {
      //could add rolling hash function to make comparison more efficient
      //but overkill in this testing application
      found = true;
      int ix = i;
      for (int j = 0; j < smallerArray.length; j++) {
        if (largerArray[ix] != smallerArray[j]) {
          found = false;
          break;
        }
        ix++;
      }
      if (found) {
        throw new Exception(message);
      }
    }
  }
}
