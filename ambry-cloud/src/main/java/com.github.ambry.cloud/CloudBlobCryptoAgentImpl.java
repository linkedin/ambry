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

import com.github.ambry.cloud.CloudBlobCryptoAgent;
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
 *    and encrypted data (see {@link EncryptedKeyAndEncryptedData} below
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
    Object keyKey = kms.getKey(context);
    ByteBuffer encryptedKey = cryptoService.encryptKey(key, keyKey);
    ByteBuffer encryptedDataBuffer = cryptoService.encrypt(buffer, key);
    ByteBuffer output = ByteBuffer.allocate(EncryptedKeyAndEncryptedData.OVERHEAD_LENGTH + encryptedKey.array().length
        + encryptedDataBuffer.array().length);
    EncryptedKeyAndEncryptedData.serialize(output, new EncryptedKeyAndEncryptedData(encryptedKey, encryptedDataBuffer));
    return output;
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer buffer) throws GeneralSecurityException {
    EncryptedKeyAndEncryptedData encryptedKeyAndEncryptedData = EncryptedKeyAndEncryptedData.deserialize(buffer);
    Object blobKeyKey = kms.getKey(context);
    Object key = cryptoService.decryptKey(encryptedKeyAndEncryptedData.encryptedKey, blobKeyKey);
    return cryptoService.decrypt(encryptedKeyAndEncryptedData.encryptedData, key);
  }

  @Override
  public String getEncryptionContext() {
    return context;
  }

  /**
   * POJO class for encrypted key and encrypted data byte buffers, also ser/deser for
   * encrypted key + encrypted data
   *
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
  static class EncryptedKeyAndEncryptedData {

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

    EncryptedKeyAndEncryptedData(ByteBuffer encryptedKey, ByteBuffer encryptedData) {
      this.encryptedKey = encryptedKey;
      this.encryptedData = encryptedData;
    }

    static void serialize(ByteBuffer outputBuffer, EncryptedKeyAndEncryptedData encryptedKeyAndEncryptedData) {
      int startOffset = outputBuffer.position();
      outputBuffer.putShort(CURRENT_VERSION);
      int encryptedKeySize = encryptedKeyAndEncryptedData.encryptedKey.array().length;
      int encryptedDataSize = encryptedKeyAndEncryptedData.encryptedData.array().length;
      outputBuffer.putInt(encryptedKeySize);
      outputBuffer.putLong(encryptedDataSize);
      outputBuffer.put(encryptedKeyAndEncryptedData.encryptedKey);
      outputBuffer.put(encryptedKeyAndEncryptedData.encryptedData);
      Crc32 crc = new Crc32();
      crc.update(outputBuffer.array(), startOffset, INITIAL_MESSAGE_LENGTH + encryptedKeySize + encryptedDataSize);
      outputBuffer.putLong(crc.getValue());
    }

    static EncryptedKeyAndEncryptedData deserialize(ByteBuffer buffer) throws GeneralSecurityException {
      int startOffset = buffer.position();
      Crc32 crc = new Crc32();
      short version = buffer.getShort();
      if (version != 1) {
        throw new GeneralSecurityException("Unrecognized version");
      }
      int encryptedKeySize = buffer.getInt();
      int encryptedDataSize = (int) buffer.getLong();
      if (encryptedKeySize < 0) {
        throw new GeneralSecurityException("Encrypted key size is less than 0");
      }
      if (encryptedDataSize < 0) {
        throw new GeneralSecurityException("Encrypted data size is less than 0");
      }
      byte[] encryptedKey = new byte[encryptedKeySize];
      byte[] encryptedData = new byte[encryptedDataSize];
      buffer.get(encryptedKey);
      buffer.get(encryptedData);
      crc.update(buffer.array(), startOffset, INITIAL_MESSAGE_LENGTH + encryptedKeySize + encryptedDataSize);
      long expectedCrc = buffer.getLong();
      long actualCrc = crc.getValue();
      if (actualCrc != expectedCrc) {
        throw new GeneralSecurityException("Encrypted blob is corrupt.  ExpectedCRC: "+expectedCrc+" ActualCRC: "+actualCrc);
      }
      return new EncryptedKeyAndEncryptedData(ByteBuffer.wrap(encryptedKey), ByteBuffer.wrap(encryptedData));
    }
  }
}
