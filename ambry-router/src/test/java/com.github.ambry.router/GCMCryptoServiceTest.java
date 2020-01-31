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
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.router.CryptoTestUtils.*;


/**
 * Tests {@link GCMCryptoService} and {@link GCMCryptoServiceFactory}
 */
public class GCMCryptoServiceTest {

  private static final int MAX_DATA_SIZE = 10000;
  private static final int MIN_DATA_SIZE = 100;
  private static final int DEFAULT_KEY_SIZE_IN_CHARS = 64;
  private static final MetricRegistry REGISTRY = new MetricRegistry();
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

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
   * Test basic encryption and decryption for random data in {@link ByteBuf}.
   * @throws Exception Any unexpected error
   */
  @Test
  public void testEncryptDecryptNettyByteBuf() throws Exception {
    // testEncryptDecryptBytes already tests the correctness of the encrypt decrypt methods with ByteBuffer, in this
    // test case, we can make the assumption that these two functions always provide correct answers.

    String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
    Properties props = getKMSProperties(key, DEFAULT_KEY_SIZE_IN_CHARS);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
    GCMCryptoService cryptoService =
        (GCMCryptoService) (new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService());
    byte[] fixedIv = new byte[12];
    for (int i = 0; i < 5; i++) {
      int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE);
      byte[] randomData = new byte[size];
      TestUtils.RANDOM.nextBytes(randomData);
      ByteBuffer toEncrypt = ByteBuffer.wrap(randomData);
      ByteBuf toEncryptByteBufHeap = ByteBufAllocator.DEFAULT.heapBuffer(size);
      ByteBuf toEncryptByteBufDirect = ByteBufAllocator.DEFAULT.ioBuffer(size);
      toEncryptByteBufHeap.writeBytes(randomData);
      toEncryptByteBufDirect.writeBytes(randomData);

      ByteBuffer encryptedBytes = cryptoService.encrypt(toEncrypt, secretKeySpec, fixedIv);
      ByteBuf encryptedBytesByteBufHeap = cryptoService.encrypt(toEncryptByteBufHeap, secretKeySpec, fixedIv);
      ByteBuf encryptedBytesByteBufDirect = cryptoService.encrypt(toEncryptByteBufDirect, secretKeySpec, fixedIv);

      // EncryptedByteBuf should be a head buffer always.
      Assert.assertTrue(encryptedBytesByteBufHeap.hasArray());
      Assert.assertTrue(encryptedBytesByteBufDirect.hasArray());
      Assert.assertEquals(encryptedBytes.remaining(), encryptedBytesByteBufHeap.readableBytes());
      Assert.assertEquals(encryptedBytes.remaining(), encryptedBytesByteBufDirect.readableBytes());
      Assert.assertEquals(toEncrypt.remaining(), 0);
      Assert.assertEquals(toEncryptByteBufDirect.readableBytes(), 0);
      Assert.assertEquals(toEncryptByteBufHeap.readableBytes(), 0);

      byte[] arrayFromByteBuf = new byte[encryptedBytesByteBufHeap.readableBytes()];
      encryptedBytesByteBufHeap.getBytes(encryptedBytesByteBufHeap.readerIndex(), arrayFromByteBuf);
      Assert.assertArrayEquals(encryptedBytes.array(), arrayFromByteBuf);
      encryptedBytesByteBufDirect.getBytes(encryptedBytesByteBufDirect.readerIndex(), arrayFromByteBuf);
      Assert.assertArrayEquals(encryptedBytes.array(), arrayFromByteBuf);

      ByteBuf toDecryptByteBufHeap = encryptedBytesByteBufHeap;
      ByteBuf toDecryptByteBufDirect = ByteBufAllocator.DEFAULT.ioBuffer(encryptedBytesByteBufHeap.readableBytes());
      toDecryptByteBufDirect.writeBytes(toDecryptByteBufHeap, 0, toDecryptByteBufHeap.readableBytes());

      ByteBuffer decryptedBytes = cryptoService.decrypt(encryptedBytes, secretKeySpec);
      ByteBuf decryptedBytesByteBufHeap = cryptoService.decrypt(toDecryptByteBufHeap, secretKeySpec);
      ByteBuf decryptedBytesByteBufDirect = cryptoService.decrypt(toDecryptByteBufDirect, secretKeySpec);

      Assert.assertTrue(decryptedBytesByteBufHeap.hasArray());
      Assert.assertTrue(decryptedBytesByteBufDirect.hasArray());
      Assert.assertEquals(decryptedBytes.remaining(), decryptedBytesByteBufHeap.readableBytes());
      Assert.assertEquals(decryptedBytes.remaining(), decryptedBytesByteBufDirect.readableBytes());
      Assert.assertEquals(encryptedBytes.remaining(), 0);
      Assert.assertEquals(toDecryptByteBufDirect.readableBytes(), 0);
      Assert.assertEquals(toDecryptByteBufHeap.readableBytes(), 0);

      arrayFromByteBuf = new byte[decryptedBytesByteBufHeap.readableBytes()];
      decryptedBytesByteBufHeap.getBytes(decryptedBytesByteBufHeap.readerIndex(), arrayFromByteBuf);
      Assert.assertArrayEquals(decryptedBytes.array(), arrayFromByteBuf);
      decryptedBytesByteBufDirect.getBytes(decryptedBytesByteBufDirect.readerIndex(), arrayFromByteBuf);
      Assert.assertArrayEquals(decryptedBytes.array(), arrayFromByteBuf);

      toEncryptByteBufHeap.release();
      toEncryptByteBufDirect.release();
      encryptedBytesByteBufHeap.release();
      encryptedBytesByteBufDirect.release();
      toDecryptByteBufDirect.release();
      decryptedBytesByteBufHeap.release();
      decryptedBytesByteBufDirect.release();
    }
  }

  @Test
  public void testEncryptDecryptNettyCompositeByteBuf() throws Exception {
    // testEncryptDecryptNettyByte already tests the correctness of the encrypt decrypt methods with non-composite Netty
    // ByteBuf, in this test case, we can make the assumption that these two functions always provide correct answers.

    String key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
    Properties props = getKMSProperties(key, DEFAULT_KEY_SIZE_IN_CHARS);
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
    GCMCryptoService cryptoService =
        (GCMCryptoService) (new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService());
    byte[] fixedIv = new byte[12];
    for (int i = 0; i < 5; i++) {
      int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE - MIN_DATA_SIZE) + MIN_DATA_SIZE;
      byte[] randomData = new byte[size];
      TestUtils.RANDOM.nextBytes(randomData);

      ByteBuf toEncrypt = Unpooled.wrappedBuffer(randomData);
      CompositeByteBuf toEncryptComposite = new CompositeByteBuf(toEncrypt.alloc(), toEncrypt.isDirect(), size);
      int start = 0;
      int end = 0;
      for (int j = 0; j < 3; j++) {
        start = end;
        end = TestUtils.RANDOM.nextInt(size / 2 - 1) + end;
        if (j == 2) {
          end = size;
        }
        ByteBuf c = Unpooled.buffer(end - start);
        c.writeBytes(randomData, start, end - start);
        toEncryptComposite.addComponent(true, c);
      }

      ByteBuf encryptedBytes = cryptoService.encrypt(toEncrypt, secretKeySpec, fixedIv);
      ByteBuf encryptedBytesComposite = cryptoService.encrypt(toEncryptComposite, secretKeySpec, fixedIv);
      Assert.assertEquals(encryptedBytes.readableBytes(), encryptedBytesComposite.readableBytes());
      Assert.assertEquals(toEncrypt.readableBytes(), 0);
      Assert.assertEquals(toEncryptComposite.readableBytes(), 0);

      byte[] array = new byte[encryptedBytes.readableBytes()];
      encryptedBytes.getBytes(encryptedBytes.readerIndex(), array);
      byte[] arrayComposite = new byte[encryptedBytesComposite.readableBytes()];
      encryptedBytesComposite.getBytes(encryptedBytesComposite.readerIndex(), arrayComposite);
      Assert.assertArrayEquals(array, arrayComposite);

      ByteBuf toDecrypt = encryptedBytes;
      CompositeByteBuf toDecryptComposite = new CompositeByteBuf(toDecrypt.alloc(), toEncrypt.isDirect(), size);
      size = encryptedBytes.readableBytes();
      start = 0;
      end = 0;
      for (int j = 0; j < 3; j++) {
        start = end;
        end = TestUtils.RANDOM.nextInt(size / 2 - 1) + end;
        if (j == 2) {
          end = size;
        }
        ByteBuf c = Unpooled.buffer(end - start);
        c.writeBytes(encryptedBytes, start, end - start);
        toDecryptComposite.addComponent(true, c);
      }

      ByteBuf decryptedBytes = cryptoService.decrypt(toDecrypt, secretKeySpec);
      ByteBuf decryptedBytesComposite = cryptoService.decrypt(toDecryptComposite, secretKeySpec);

      Assert.assertEquals(decryptedBytes.readableBytes(), decryptedBytesComposite.readableBytes());
      Assert.assertEquals(toDecrypt.readableBytes(), 0);
      Assert.assertEquals(toDecryptComposite.readableBytes(), 0);

      array = new byte[decryptedBytes.readableBytes()];
      arrayComposite = new byte[decryptedBytesComposite.readableBytes()];
      decryptedBytes.getBytes(decryptedBytes.readerIndex(), array);
      decryptedBytesComposite.getBytes(decryptedBytesComposite.readerIndex(), arrayComposite);
      Assert.assertArrayEquals(array, arrayComposite);

      toEncrypt.release();
      toEncryptComposite.release();
      encryptedBytes.release();
      encryptedBytesComposite.release();
      toDecryptComposite.release();
      decryptedBytes.release();
      decryptedBytesComposite.release();
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
