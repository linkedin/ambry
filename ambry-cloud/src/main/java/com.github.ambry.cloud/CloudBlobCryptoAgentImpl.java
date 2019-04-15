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

import com.github.ambry.router.CryptoService;
import com.github.ambry.router.KeyManagementService;
import com.github.ambry.utils.Crc32;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;


/**
 * Implementation of CloudBlobCryptoAgent, encrypts byte buffers by
 * 1. generating a random encryption key
 * 2. encrypting that random key with a key from KeyManagementService
 *    that is associated with the given context string
 * 3. encrypting a data byte buffer with the generated random key
 * 4. returning a byte buffer containing the serialized encrypted key
 *    and encrypted data (see {@link EncryptedDataPayload} below
 *    for serialization description
 */
public class CloudBlobCryptoAgentImpl implements CloudBlobCryptoAgent {

  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  //Use this context to look up encryption key for the data encryption key
  private final String context;

  public CloudBlobCryptoAgentImpl(CryptoService cryptoService, KeyManagementService kms, String context) {
    this.cryptoService = cryptoService;
    this.kms = kms;
    this.context = context;
  }

  @Override
  public ByteBuffer encrypt(ByteBuffer buffer) throws GeneralSecurityException {
    Object key = kms.getRandomKey();
    Object contextKey = kms.getKey(context);
    ByteBuffer encryptedKey = cryptoService.encryptKey(key, contextKey);
    ByteBuffer encryptedDataBuffer = cryptoService.encrypt(buffer, key);
    ByteBuffer output = ByteBuffer.allocate(
        EncryptedDataPayload.OVERHEAD_LENGTH + encryptedKey.array().length + encryptedDataBuffer.array().length);
    EncryptedDataPayload.serialize(output, new EncryptedDataPayload(encryptedKey, encryptedDataBuffer));
    return output;
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer buffer) throws GeneralSecurityException {
    EncryptedDataPayload encryptedDataPayload = EncryptedDataPayload.deserialize(buffer);
    Object blobKeyKey = kms.getKey(context);
    Object key = cryptoService.decryptKey(encryptedDataPayload.encryptedKey, blobKeyKey);
    return cryptoService.decrypt(encryptedDataPayload.encryptedData, key);
  }

  @Override
  public String getEncryptionContext() {
    return context;
  }

  /**
   * POJO class representing payload for encrypted key and data payload, with ser/deser methods.
   *
   *  - - - - - - - - - - - - - - - - - - -- - -- - - - - - -- - - - - -- - - - - -
   * |         |              |                 |           |          |          |
   * | version |  Encrypted   |  Encrypted      | Encrypted | Encrypted|Crc       |
   * |(2 bytes)|  Key Size    |  Data Size      | Key       | Data     |(8 bytes) |
   * |         |              |                 |           | (m bytes)|          |
   * |         |  (4 bytes)   | (8 bytes)       | (n bytes) |          |          |
   * |         |              |                 |           |          |          |
   *  - - - - - - - - - - - - - - - - - - -- - -- - - - - - -- - - - - -- - - - - -
   *
   *  version         - The version of the message header
   *
   *  encrypted key   - The size of the encrypted key (should be >= 0).
   *  size
   *
   *  encrypted data  - The size of the encrypted data (should be >= 0).
   *  size              8 bytes for future proofing, but current implementation only
   *                    supports int sized sizes
   *
   *  encrypted key   - The encrypted key
   *
   *  encrypted data  - The encrypted data
   *
   *  crc             - The crc of the message
   *
   */
  static class EncryptedDataPayload {

    private static short CURRENT_VERSION = 1;
    private static int VERSION_FIELD_SIZE = 2;
    private static int ENCRYPTED_KEY_SIZE_FIELD_SIZE = 4;
    private static int ENCRYPTED_DATA_SIZE_FIELD_SIZE = 8;
    static int INITIAL_MESSAGE_LENGTH =
        VERSION_FIELD_SIZE + ENCRYPTED_KEY_SIZE_FIELD_SIZE + ENCRYPTED_DATA_SIZE_FIELD_SIZE;
    private static int CRC_FIELD_LENGTH = 8;
    static int OVERHEAD_LENGTH = INITIAL_MESSAGE_LENGTH + CRC_FIELD_LENGTH;

    private ByteBuffer encryptedKey;
    private ByteBuffer encryptedData;

    /**
     * Constructor.
     * @param encryptedKey the buffer containing the encrypted key.
     * @param encryptedData the buffer containing the encrypted data.
     */
    EncryptedDataPayload(ByteBuffer encryptedKey, ByteBuffer encryptedData) {
      this.encryptedKey = encryptedKey;
      this.encryptedData = encryptedData;
    }

    /**
     * Serialize the {@link EncryptedDataPayload} into an output buffer.
     * @param outputBuffer the {@link ByteBuffer} to write to.
     * @param encryptedDataPayload the {@link EncryptedDataPayload} to serialize.
     */
    static void serialize(ByteBuffer outputBuffer, EncryptedDataPayload encryptedDataPayload) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(CURRENT_VERSION);
      int encryptedKeySize = encryptedDataPayload.encryptedKey.array().length;
      int encryptedDataSize = encryptedDataPayload.encryptedData.array().length;
      outputBuffer.putInt(encryptedKeySize);
      outputBuffer.putLong(encryptedDataSize);
      outputBuffer.put(encryptedDataPayload.encryptedKey);
      outputBuffer.put(encryptedDataPayload.encryptedData);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, INITIAL_MESSAGE_LENGTH + encryptedKeySize + encryptedDataSize);
      outputBuffer.putLong(crc.getValue());
    }

    /**
     * Deserialize an {@link EncryptedDataPayload} from an input buffer.
     * @param inputBuffer the {@link ByteBuffer} to read from.
     * @return the deserialized {@link EncryptedDataPayload}.
     */
    static EncryptedDataPayload deserialize(ByteBuffer inputBuffer) throws GeneralSecurityException {
      int startOffset = inputBuffer.position();
      Crc32 crc = new Crc32();
      short version = inputBuffer.getShort();
      if (version != 1) {
        throw new GeneralSecurityException("Unrecognized version: " + version);
      }
      int encryptedKeySize = inputBuffer.getInt();
      int encryptedDataSize = (int) inputBuffer.getLong();
      if (encryptedKeySize < 0) {
        throw new GeneralSecurityException("Encrypted key size is less than 0");
      }
      if (encryptedDataSize < 0) {
        throw new GeneralSecurityException("Encrypted data size is less than 0");
      }
      byte[] encryptedKey = new byte[encryptedKeySize];
      byte[] encryptedData = new byte[encryptedDataSize];
      try {
        inputBuffer.get(encryptedKey);
      } catch (Exception e) {
        throw new GeneralSecurityException("Reading encrypted key from buffer", e);
      }
      try {
        inputBuffer.get(encryptedData);
      } catch (Exception e) {
        throw new GeneralSecurityException("Reading encrypted data from buffer", e);
      }
      crc.update(inputBuffer.array(), startOffset, INITIAL_MESSAGE_LENGTH + encryptedKeySize + encryptedDataSize);
      long expectedCrc;
      try {
        expectedCrc = inputBuffer.getLong();
      } catch (Exception e) {
        throw new GeneralSecurityException("Reading crc from buffer", e);
      }
      long actualCrc = crc.getValue();
      if (actualCrc != expectedCrc) {
        throw new GeneralSecurityException(
            "Encrypted blob is corrupt.  ExpectedCRC: " + expectedCrc + " ActualCRC: " + actualCrc);
      }
      return new EncryptedDataPayload(ByteBuffer.wrap(encryptedKey), ByteBuffer.wrap(encryptedData));
    }
  }
}
