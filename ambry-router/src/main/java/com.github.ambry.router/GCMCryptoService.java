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

import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.Security;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link CryptoService} which is capable of encrypting or decrypting bytes based on the given key.
 * This implementation uses GCM for encryption and decryption
 */
public class GCMCryptoService implements CryptoService<SecretKeySpec> {

  private static final int VERSION_FIELD_SIZE_IN_BYTES = Short.BYTES;
  private static final short KEY_RECORD_VERSION_V_1 = 1;
  private static final short IV_RECORD_VERSION_V_1 = 1;
  private static final String GCM_CRYPTO_INSTANCE = "AES/GCM/NoPadding";
  private final SecureRandom random = new SecureRandom();
  private final int ivValSize;
  private final CryptoServiceConfig config;

  private static final Logger logger = LoggerFactory.getLogger(GCMCryptoService.class);

  public GCMCryptoService(CryptoServiceConfig cryptoServiceConfig) {
    config = cryptoServiceConfig;
    ivValSize = cryptoServiceConfig.cryptoServiceIvSizeInBytes;
    Security.addProvider(new BouncyCastleProvider());
    if (!config.cryptoServiceEncryptionDecryptionMode.equals("GCM")) {
      throw new IllegalArgumentException(
          "Unrecognized Encryption Decryption Mode " + config.cryptoServiceEncryptionDecryptionMode);
    }
  }

  @Override
  public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
    return encrypt(toEncrypt, key, null);
  }

  /**
   * Helper function to encrypt ByteBuffer with the given key. When useFixedIv is true, don't use a random iv byte
   * array, using a all zero byte array instead. Only set it to be true in test.
   * @param toEncrypt {@link ByteBuffer} that needs to be encrypted
   * @param key the secret key (of type T) to use to encrypt
   * @param iv If null, will create a random byte array serve as iv bytes.
   * @return the {@link ByteBuffer} containing the encrypted content. Ensure the result has all
   * the information like the IV along with the encrypted content, in order to decrypt the content with a given key
   * @throws {@link GeneralSecurityException} on any exception with encryption
   */
  ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key, byte[] iv) throws GeneralSecurityException {
    try {
      Cipher encrypter = Cipher.getInstance(GCM_CRYPTO_INSTANCE, "BC");
      if (iv == null) {
        iv = new byte[ivValSize];
        random.nextBytes(iv);
      }
      encrypter.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
      int outputSize = encrypter.getOutputSize(toEncrypt.remaining());
      ByteBuffer encryptedContent = ByteBuffer.allocate(IVRecord_Format_V1.getIVRecordSize(iv) + outputSize);
      IVRecord_Format_V1.serializeIVRecord(encryptedContent, iv);
      encrypter.doFinal(toEncrypt, encryptedContent);
      encryptedContent.flip();
      return encryptedContent;
    } catch (Exception e) {
      throw new GeneralSecurityException("Exception thrown while encrypting data", e);
    }
  }

  @Override
  public ByteBuf encrypt(ByteBuf toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
    return encrypt(toEncrypt, key, null);
  }

  /**
   * Helper function to encrypt ByteBuf with the given key. When useFixedIv is true, don't use a random iv byte
   * array, using a all zero byte array instead. Only set it to be true in test.
   * @param toEncrypt {@link ByteBuf} that needs to be encrypted
   * @param key the secret key (of type T) to use to encrypt
   * @param iv If null, will create a random byte array serve as iv bytes.
   * @return the {@link ByteBuf} containing the encrypted content. Ensure the result has all
   * the information like the IV along with the encrypted content, in order to decrypt the content with a given key
   * @throws {@link GeneralSecurityException} on any exception with encryption
   */
  public ByteBuf encrypt(ByteBuf toEncrypt, SecretKeySpec key, byte[] iv) throws GeneralSecurityException {
    try {
      Cipher encrypter = Cipher.getInstance(GCM_CRYPTO_INSTANCE, "BC");
      if (iv == null) {
        iv = new byte[ivValSize];
        random.nextBytes(iv);
      }
      encrypter.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
      int outputSize = encrypter.getOutputSize(toEncrypt.readableBytes());

      // stick with heap memory for now so to compare with the java.nio.ByteBuffer.
      ByteBuf encryptedContent =
          ByteBufAllocator.DEFAULT.heapBuffer(IVRecord_Format_V1.getIVRecordSize(iv) + outputSize);
      IVRecord_Format_V1.serializeIVRecord(encryptedContent, iv);

      boolean toRelease = false;
      if (toEncrypt.nioBufferCount() != 1) {
        toRelease = true;
        ByteBuf temp = ByteBufAllocator.DEFAULT.heapBuffer(toEncrypt.readableBytes());
        temp.writeBytes(toEncrypt);
        toEncrypt = temp;
      }
      try {
        ByteBuffer toEncryptBuffer = toEncrypt.nioBuffer();
        ByteBuffer encryptedContentBuffer = encryptedContent.nioBuffer(encryptedContent.writerIndex(),
            encryptedContent.capacity() - encryptedContent.writerIndex());
        int n = encrypter.doFinal(toEncryptBuffer, encryptedContentBuffer);
        encryptedContent.writerIndex(encryptedContent.writerIndex() + n);
        return encryptedContent;
      } finally {
        if (toRelease) {
          toEncrypt.release();
        } else {
          toEncrypt.skipBytes(toEncrypt.readableBytes());
        }
      }
    } catch (Exception e) {
      throw new GeneralSecurityException("Exception thrown while encrypting data", e);
    }
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
    try {
      Cipher decrypter = Cipher.getInstance(GCM_CRYPTO_INSTANCE, "BC");
      byte[] iv = deserializeIV(new ByteBufferInputStream(toDecrypt));
      decrypter.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
      ByteBuffer decryptedContent = ByteBuffer.allocate(decrypter.getOutputSize(toDecrypt.remaining()));
      decrypter.doFinal(toDecrypt, decryptedContent);
      decryptedContent.flip();
      return decryptedContent;
    } catch (Exception e) {
      throw new GeneralSecurityException("Exception thrown while decrypting data", e);
    }
  }

  @Override
  public ByteBuf decrypt(ByteBuf toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
    try {
      Cipher decrypter = Cipher.getInstance(GCM_CRYPTO_INSTANCE, "BC");
      byte[] iv = deserializeIV(new ByteBufInputStream(toDecrypt));
      decrypter.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
      int outputSize = decrypter.getOutputSize(toDecrypt.readableBytes());
      ByteBuf decryptedContent = ByteBufAllocator.DEFAULT.heapBuffer(outputSize);

      boolean toRelease = false;
      if (toDecrypt.nioBufferCount() != 1) {
        toRelease = true;
        ByteBuf temp = ByteBufAllocator.DEFAULT.heapBuffer(toDecrypt.readableBytes());
        temp.writeBytes(toDecrypt);
        toDecrypt = temp;
      }

      try {
        ByteBuffer toDecryptBuffer = toDecrypt.nioBuffer();
        ByteBuffer decryptedContentBuffer = decryptedContent.nioBuffer(0, outputSize);
        int n = decrypter.doFinal(toDecryptBuffer, decryptedContentBuffer);
        decryptedContent.writerIndex(decryptedContent.writerIndex() + n);
        return decryptedContent;
      } finally {
        if (toRelease) {
          toDecrypt.release();
        } else {
          toDecrypt.skipBytes(toDecrypt.readableBytes());
        }
      }
    } catch (Exception e) {
      throw new GeneralSecurityException("Exception thrown while decrypting data", e);
    }
  }

  @Override
  public ByteBuffer encryptKey(SecretKeySpec toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
    byte[] encodedKey = toEncrypt.getEncoded();
    ByteBuffer keyRecordBuffer =
        ByteBuffer.allocate(KeyRecord_Format_V1.getKeyRecordSize(encodedKey, toEncrypt.getAlgorithm()));
    KeyRecord_Format_V1.serializeKeyRecord(keyRecordBuffer, encodedKey, toEncrypt.getAlgorithm());
    keyRecordBuffer.flip();
    return encrypt(keyRecordBuffer, key);
  }

  @Override
  public SecretKeySpec decryptKey(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
    ByteBuffer decryptedKey = decrypt(toDecrypt, key);
    DeserializedKey deserializedKey = null;
    try {
      deserializedKey = deserializeKey(new ByteBufferInputStream(decryptedKey));
    } catch (Exception e) {
      throw new GeneralSecurityException("Exception thrown on deserializing key");
    }
    return new SecretKeySpec(deserializedKey.getEncodedKey(), deserializedKey.getKeyGenAlgo());
  }

  /**
   * Deserialize IV from the stream
   * @param stream the stream from which IV needs to be deserialized
   * @return the IV of type byte array thus deserialized
   * @throws IOException
   * @throws MessageFormatException
   */
  private static byte[] deserializeIV(InputStream stream) throws IOException, MessageFormatException {
    DataInputStream inputStream = new DataInputStream(stream);
    short version = inputStream.readShort();
    switch (version) {
      case IV_RECORD_VERSION_V_1:
        return IVRecord_Format_V1.deserializeIVRecord(inputStream);
      default:
        throw new MessageFormatException("IVRecord version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  /**
   * Deserialize key from the stream
   * @param stream the {@link InputStream} from which key needs to be deserialized
   * @return the deserialized key in the form of {@link DeserializedKey}
   * @throws IOException
   * @throws MessageFormatException
   */
  private static DeserializedKey deserializeKey(InputStream stream) throws IOException, MessageFormatException {
    DataInputStream inputStream = new DataInputStream(stream);
    short version = inputStream.readShort();
    switch (version) {
      case KEY_RECORD_VERSION_V_1:
        return KeyRecord_Format_V1.deserializeKeyRecord(inputStream);
      default:
        throw new MessageFormatException("KeyRecord version not supported",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - -
   * |         |           |            |
   * | version |   size    |  content   |
   * |(2 bytes)| (4 bytes) |  (n bytes) |
   * |         |           |            |
   *  - - - - - - - - - - - - - - - - -
   *  version    - The version of the IV record
   *
   *  size       - The size of the IV
   *
   *  content    - The actual IV
   *
   */
  private static class IVRecord_Format_V1 {
    private static final int IV_SIZE_FIELD_IN_BYTES = Integer.BYTES;

    /**
     * Returns the IV record size for the given iv
     * @param iv the iv for which IV record size is requested
     * @return the size of the IV record size for the given iv
     */
    private static int getIVRecordSize(byte[] iv) {
      return VERSION_FIELD_SIZE_IN_BYTES + IV_SIZE_FIELD_IN_BYTES + iv.length;
    }

    /**
     * Serialize IV into {@link ByteBuffer}
     * @param outputBuffer the {@link ByteBuffer} to which the IV needs to be serialized
     * @param iv the iv of type byte array that needs to be serialized
     */
    private static void serializeIVRecord(ByteBuffer outputBuffer, byte[] iv) {
      outputBuffer.putShort(IV_RECORD_VERSION_V_1);
      outputBuffer.putInt(iv.length);
      outputBuffer.put(iv);
    }

    /**
     * Serialize IV into {@link ByteBuf}
     * @param outputBuf the {@link ByteBuf} to which the IV needs to be serialized
     * @param iv the iv of type byte array that needs to be serialized
     */
    private static void serializeIVRecord(ByteBuf outputBuf, byte[] iv) {
      outputBuf.writeShort(IV_RECORD_VERSION_V_1);
      outputBuf.writeInt(iv.length);
      outputBuf.writeBytes(iv);
    }

    /**
     * Deserializes the iv from the stream
     * @param dataStream the {@link DataInputStream} from which iv needs to be deserialized
     * @return the iv thus deserialized from the stream
     * @throws IOException
     * @throws MessageFormatException
     */
    private static byte[] deserializeIVRecord(DataInputStream dataStream) throws IOException, MessageFormatException {
      int ivSize = dataStream.readInt();
      return Utils.readBytesFromStream(dataStream, ivSize);
    }
  }

  /**
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * |         |           |            |               |              |
   * | version | key size  |  content   |  key gen algo | key gen Algo |
   * |(2 bytes)| (4 bytes) |  (n bytes) |      size     |   (n bytes)  |
   * |         |           |            |   (4 bytes)   |              |
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version             - The version of the Key Record
   *
   *  key size            - The size of the key
   *
   *  content             - The actual key
   *
   *  key gen algo size   - Size of the key gen algo
   *
   *  key gen algo        - The actual key gen algo
   */
  private static class KeyRecord_Format_V1 {
    private static final int KEY_SIZE_FIELD_IN_BYTES = Integer.BYTES;

    /**
     * Returns the key record size for the given key and key gen algo
     * @param key the key for which key record size is requested
     * @param keyGenAlgo the key gen algo used to generate the key
     * @return the size of key record for the given key and key gen algo
     */
    private static int getKeyRecordSize(byte[] key, String keyGenAlgo) {
      return VERSION_FIELD_SIZE_IN_BYTES + KEY_SIZE_FIELD_IN_BYTES + key.length + Utils.getIntStringLength(keyGenAlgo);
    }

    /**
     * Serialize {@code key} and {@code keyGenAlgo} to {@link ByteBuffer} in {@link KeyRecord_Format_V1}
     * @param outputBuffer the {@link ByteBuffer} to which the key and key gen algo needs to be serialized
     * @param key the key that needs to be serialized
     * @param keyGenAlgo the key gen algo that needs to be serialized
     */
    private static void serializeKeyRecord(ByteBuffer outputBuffer, byte[] key, String keyGenAlgo) {
      outputBuffer.putShort(KEY_RECORD_VERSION_V_1);
      outputBuffer.putInt(key.length);
      outputBuffer.put(key);
      Utils.serializeString(outputBuffer, keyGenAlgo, StandardCharsets.US_ASCII);
    }

    /**
     * Deserialize key from the stream in {@link KeyRecord_Format_V1}
     * @param dataStream the {@link DataInputStream} from which key needs to be deserialized
     * @return the deserialized key in the form of {@link DeserializedKey}
     * @throws IOException
     * @throws MessageFormatException
     */
    private static DeserializedKey deserializeKeyRecord(DataInputStream dataStream)
        throws IOException, MessageFormatException {
      int keySize = dataStream.readInt();
      byte[] encodedKey = new byte[keySize];
      dataStream.read(encodedKey);
      String keyAlgo = Utils.readIntString(dataStream);
      return new DeserializedKey(encodedKey, keyAlgo);
    }
  }

  /**
   * Class to hold the encoded key and the key gen algo
   */
  private static class DeserializedKey {
    private final byte[] encodedKey;
    private final String keyGenAlgo;

    DeserializedKey(byte[] encodedKey, String keyGenAlgo) {
      this.encodedKey = encodedKey;
      this.keyGenAlgo = keyGenAlgo;
    }

    byte[] getEncodedKey() {
      return encodedKey;
    }

    String getKeyGenAlgo() {
      return keyGenAlgo;
    }
  }
}

