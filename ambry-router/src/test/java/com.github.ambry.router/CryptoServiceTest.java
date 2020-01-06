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
import java.util.List;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.CryptoTestUtils.*;


/**
 * Tests for default method in interface {@link CryptoService}.
 */
@RunWith(Parameterized.class)
public class CryptoServiceTest {
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();
  private static final MetricRegistry REGISTRY = new MetricRegistry();
  private static final int DEFAULT_KEY_SIZE_IN_CHARS = 64;
  private static final int MAX_DATA_SIZE = 10000;
  private static final int MIN_DATA_SIZE = 100;
  private final boolean isCompositeByteBuf;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Constructor to create a CryptoServiceTest.
   * @param isCompositeByteBuf
   */
  public CryptoServiceTest(boolean isCompositeByteBuf) {
    this.isCompositeByteBuf = isCompositeByteBuf;
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Create a {@link CompositeByteBuf} from the given byte array.
   * @param data the byte array.
   * @return A {@link CompositeByteBuf}.
   */
  private CompositeByteBuf fromByteArrayToCompositeByteBuf(byte[] data) {
    int size = data.length;
    ByteBuf toEncrypt = Unpooled.wrappedBuffer(data);
    CompositeByteBuf composite = new CompositeByteBuf(toEncrypt.alloc(), toEncrypt.isDirect(), size);
    int start = 0;
    int end = 0;
    for (int j = 0; j < 3; j++) {
      start = end;
      end = TestUtils.RANDOM.nextInt(size / 2 - 1) + end;
      if (j == 2) {
        end = size;
      }
      ByteBuf c = Unpooled.buffer(end - start);
      c.writeBytes(data, start, end - start);
      composite.addComponent(true, c);
    }
    return composite;
  }

  /**
   * Create a {@link ByteBuf} based on whether it should be a composite ByteBuf or not. If it should, then
   * create a {@link CompositeByteBuf} with three components.
   * @return A {@link ByteBuf}.
   */
  private ByteBuf createByteBuf() {
    int size = TestUtils.RANDOM.nextInt(MAX_DATA_SIZE - MIN_DATA_SIZE) + MIN_DATA_SIZE;
    byte[] randomData = new byte[size];
    TestUtils.RANDOM.nextBytes(randomData);
    if (isCompositeByteBuf) {
      return fromByteArrayToCompositeByteBuf(randomData);
    } else {
      return ByteBufAllocator.DEFAULT.heapBuffer(size);
    }
  }

  /**
   * Convert the given {@link ByteBuf} to a {@link CompositeByteBuf} if the {@code isCompositeByteBuf} is true.
   * @param buf The given {@link ByteBuf}.
   * @return The result {@link ByteBuf}.
   */
  private ByteBuf maybeConvertToComposite(ByteBuf buf) {
    if (!isCompositeByteBuf) {
      return buf.retainedDuplicate();
    } else {
      byte[] data = new byte[buf.readableBytes()];
      buf.getBytes(buf.readerIndex(), data);
      return fromByteArrayToCompositeByteBuf(data);
    }
  }

  /**
   * Create a {@link ByteBuffer} from given {@link ByteBuf} so that they have the same content.
   * @param byteBuf The given {@link ByteBuf}....
   * @return The {@link ByteBuffer}.
   */
  private ByteBuffer fromByteBufToByteBuffer(ByteBuf byteBuf) {
    int size = byteBuf.readableBytes();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    byteBuf.getBytes(0, buffer);
    buffer.flip();
    return buffer;
  }

  /**
   * Test the default methods for those implementations that don't implement the default methods.
   * @throws Exception
   */
  @Test
  public void testDefaultMethodForEncryptDecrypt() throws Exception {
    CryptoService<SecretKeySpec> cryptoService = new MockCryptoService();
    String key = ((MockCryptoService) cryptoService).getKey();
    SecretKeySpec secretKeySpec = new SecretKeySpec(Hex.decode(key), "AES");
    for (int i = 0; i < 5; i++) {
      ByteBuf toEncryptByteBuf = createByteBuf();
      ByteBuffer toEncrypt = fromByteBufToByteBuffer(toEncryptByteBuf);
      ByteBuf encryptedBytesByteBuf = cryptoService.encrypt(toEncryptByteBuf, secretKeySpec);
      ByteBuffer encryptedBytes = cryptoService.encrypt(toEncrypt, secretKeySpec);

      Assert.assertTrue(encryptedBytesByteBuf.hasArray());
      Assert.assertEquals(encryptedBytes.remaining(), encryptedBytesByteBuf.readableBytes());
      Assert.assertEquals(toEncryptByteBuf.readableBytes(), 0);
      Assert.assertEquals(toEncrypt.remaining(), 0);
      byte[] arrayFromByteBuf = new byte[encryptedBytesByteBuf.readableBytes()];
      encryptedBytesByteBuf.getBytes(encryptedBytesByteBuf.readerIndex(), arrayFromByteBuf);
      Assert.assertArrayEquals(encryptedBytes.array(), arrayFromByteBuf);

      ByteBuf toDecryptByteBuf = maybeConvertToComposite(encryptedBytesByteBuf);
      ByteBuffer toDecrypt = encryptedBytes;
      ByteBuf decryptedBytesByteBuf = cryptoService.decrypt(toDecryptByteBuf, secretKeySpec);
      ByteBuffer decryptedBytes = cryptoService.decrypt(encryptedBytes, secretKeySpec);

      Assert.assertTrue(decryptedBytesByteBuf.hasArray());
      Assert.assertEquals(decryptedBytes.remaining(), decryptedBytesByteBuf.readableBytes());
      Assert.assertEquals(toDecryptByteBuf.readableBytes(), 0);
      Assert.assertEquals(toDecrypt.remaining(), 0);
      arrayFromByteBuf = new byte[decryptedBytesByteBuf.readableBytes()];
      decryptedBytesByteBuf.getBytes(decryptedBytesByteBuf.readerIndex(), arrayFromByteBuf);
      Assert.assertArrayEquals(decryptedBytes.array(), arrayFromByteBuf);

      toEncryptByteBuf.release();
      encryptedBytesByteBuf.release();
      toDecryptByteBuf.release();
      decryptedBytesByteBuf.release();
    }
  }

  /**
   * A mock {@link CryptoService} that doesn't implements default methods.
   */
  static class MockCryptoService implements CryptoService<SecretKeySpec> {
    private GCMCryptoService cryptoService;
    private final String key;
    private final byte[] fixedIv;

    public MockCryptoService() {
      key = TestUtils.getRandomKey(DEFAULT_KEY_SIZE_IN_CHARS);
      Properties props = getKMSProperties(key, DEFAULT_KEY_SIZE_IN_CHARS);
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      cryptoService = (GCMCryptoService) new GCMCryptoServiceFactory(verifiableProperties, REGISTRY).getCryptoService();
      fixedIv = new byte[12];
    }

    @Override
    public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      return cryptoService.encrypt(toEncrypt, key, fixedIv);
    }

    @Override
    public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      return cryptoService.decrypt(toDecrypt, key);
    }

    @Override
    public ByteBuffer encryptKey(SecretKeySpec toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      return cryptoService.encryptKey(toEncrypt, key);
    }

    @Override
    public SecretKeySpec decryptKey(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      return cryptoService.decryptKey(toDecrypt, key);
    }

    public String getKey() {
      return key;
    }
  }
}
