/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.compression;

import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BaseCompressionTest {

  @Test
  public void testGetAlgorithmNameBinary() throws Throwable {
    // Test: A single-letter algorithm name.
    BaseCompression compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("A");
    byte[] binary = (byte[]) MethodUtils.invokeMethod(compressor, true, "getAlgorithmNameBinary");
    Assert.assertEquals(1, binary.length);
    Assert.assertEquals((int) 'A', binary[0]);

    // Test: Large 10 char algorithm name.
    compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("0123456789");
    binary = (byte[]) MethodUtils.invokeMethod(compressor, true, "getAlgorithmNameBinary");
    Assert.assertEquals(10, binary.length);
    Assert.assertEquals((int) '0', binary[0]);
    Assert.assertEquals((int) '9', binary[9]);

    // Test: Error case - no algorithm name.
    compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("");
    Throwable ex = TestUtils.getPrivateException(compressor, "getAlgorithmNameBinary");
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Error case - String too long.
    compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("0123456789x");
    ex = TestUtils.getPrivateException(compressor, "getAlgorithmNameBinary");
    Assert.assertTrue(ex instanceof IllegalArgumentException);
  }

  @Test
  public void testGetCompressBufferSize() {
    BaseCompression compressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);
    Mockito.when(compressor.estimateMaxCompressedDataSize(Mockito.anyInt())).thenReturn(100);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("ABCDE");
    // The expected buffer size 5 (name) + 6 + 100 = 111.
    int bufferSize = compressor.getCompressBufferSize(10);
    Assert.assertEquals(111, bufferSize);
  }

  @Test
  public void testGetDecompressBufferSize() throws CompressionException {
    byte[] compressedBuffer = new byte[10];
    compressedBuffer[0] = 10; // Bad version
    compressedBuffer[1] = 1;  // Name size.
    compressedBuffer[2] = 66; // Name is one-char long.
    compressedBuffer[3] = 0;  // original size (Big-endian encoding).
    compressedBuffer[4] = 0;
    compressedBuffer[5] = 0;
    compressedBuffer[6] = 3;
    compressedBuffer[7] = 1;   // compressed data

    BaseCompression decompressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);

    // Test: Invalid version in compressed buffer.
    ByteBuffer sourceBuffer = ByteBuffer.wrap(compressedBuffer);
    Exception ex = TestUtils.getException(() -> decompressor.getDecompressBufferSize(sourceBuffer));
    Assert.assertTrue(ex instanceof CompressionException);

    // Test: Valid compressed buffer.
    compressedBuffer[0] = 1;  // Fix version
    int originalSize = decompressor.getDecompressBufferSize(sourceBuffer);
    Assert.assertEquals(3, originalSize);
  }

  @Test
  public void testCompress() throws CompressionException {
    BaseCompression compressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);
    Mockito.when(compressor.estimateMaxCompressedDataSize(Mockito.anyInt())).thenReturn(100);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("ABCD");

    // Test: Compress empty buffer should throw.
    Throwable ex = TestUtils.getException(() -> compressor.compress(null));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    ex = TestUtils.getException(() -> compressor.compress(ByteBuffer.wrap(new byte[1]), null));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Compressed buffer size of 4 is too small.
    ex = TestUtils.getException(() -> compressor.compress(ByteBuffer.wrap(new byte[10]), ByteBuffer.wrap(new byte[4])));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Valid case - compressed data is simply a prefix plus the source data.
    Mockito.when(compressor.compressNative(Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt(),
            Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt()))
        .thenAnswer(invocation -> {
          ByteBuffer sourceBuffer = invocation.getArgument(0);
          int sourceBufferOffset = invocation.getArgument(1);
          int sourceBufferSize = invocation.getArgument(2);
          ByteBuffer targetBuffer = invocation.getArgument(3);
          int targetBufferOffset = invocation.getArgument(4);

          // In this test, just copy sourceBuffer to compressedBuffer.
          byte[] temp = new byte[sourceBufferSize];
          sourceBuffer.position(sourceBufferOffset);
          sourceBuffer.get(temp);
          targetBuffer.position(targetBufferOffset);
          targetBuffer.put(temp);
          return sourceBufferSize;
        });

    ByteBuffer testSourceData = ByteBuffer.wrap(new byte[] { 100, 101, 102 });
    ByteBuffer compressedBuffer = compressor.compress(testSourceData, false);
    // Make sure the format is
    // - 1 byte version.
    // - 1 byte name size
    // - N bytes for name (4 bytes in this case)
    // - 4 bytes original size (testSourceData size)
    // - X bytes of compressed data (3 bytes in this case).
    Assert.assertEquals(13, compressedBuffer.remaining());
    Assert.assertEquals(110, compressedBuffer.capacity());  //100 + 6 bytes overhead + 4 bytes name.
    Assert.assertEquals(1, compressedBuffer.get(0));
    Assert.assertEquals(4, compressedBuffer.get(1));
    Assert.assertEquals(0, compressedBuffer.get(6));
    Assert.assertEquals(0, compressedBuffer.get(7));
    Assert.assertEquals(0, compressedBuffer.get(8));
    Assert.assertEquals(3, compressedBuffer.get(9));
    Assert.assertEquals(testSourceData.get(0), compressedBuffer.get(10));
    Assert.assertEquals(testSourceData.get(1), compressedBuffer.get(11));
    Assert.assertEquals(testSourceData.get(2), compressedBuffer.get(12));
  }

  @Test
  public void testDecompress() throws CompressionException {
    BaseCompression decompressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);

    // Test: decompress empty buffer.
    Throwable ex = TestUtils.getException(() -> decompressor.decompress(null));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    ex = TestUtils.getException(() -> decompressor.decompress(ByteBuffer.wrap(new byte[0]), false));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    //  Note the compressed buffer format is:
    // - 1 byte version
    // - 1 byte name size
    // - N bytes for name (5 bytes in this case)
    // - 4 bytes original size (testSourceData size)
    // - X bytes of compressed data (6 bytes in this case).

    // Test: decompress an invalid buffer that is too small.  Version is 1, Name size is 10, but only 2 bytes long.
    ex = TestUtils.getException(() -> decompressor.decompress(ByteBuffer.wrap(new byte[] { 1, 10, 66 }), false));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Another invalid buffer case - Version is 1, Name size is 2, 2-char name, but original size incomplete.
    ex = TestUtils.getException(() -> decompressor.decompress(ByteBuffer.wrap(new byte[] { 1, 2, 66, 67, 3, 0 , 0}), false));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: decompress a valid compressed buffer, but source data buffer size (2) is too small.  Original size = 3.
    // Version is 1, name length is 2, 2-char name, size is 3, 3 bytes of compressed data.
    final ByteBuffer testCompressedBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 66, 67, 0, 0, 0, 3, 100, 101, 102});
    ex = TestUtils.getException(() -> decompressor.decompress(testCompressedBuffer, ByteBuffer.wrap(new byte[2])));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: decompressNative() simply copies data compressed buffer to decompressed buffer.
    Mockito.doAnswer(invocation -> {
      ByteBuffer compressedBuffer = invocation.getArgument(0);
      int compressedBufferOffset = invocation.getArgument(1);
      int compressedBufferSize = invocation.getArgument(2);
      ByteBuffer decompressedBuffer = invocation.getArgument(3);
      int decompressedBufferOffset = invocation.getArgument(4);

      // In this test, just copy compressed data to sourceBuffer.
      byte[] temp = new byte[compressedBufferSize];
      compressedBuffer.position(compressedBufferOffset);
      compressedBuffer.get(temp);
      decompressedBuffer.position(decompressedBufferOffset);
      decompressedBuffer.put(temp);
      return null;
    }).when(decompressor).decompressNative(Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt());

    ByteBuffer originalData = decompressor.decompress(testCompressedBuffer, false);
    Assert.assertEquals(3, originalData.remaining());
    Assert.assertEquals(100, originalData.get(0));
    Assert.assertEquals(101, originalData.get(1));
    Assert.assertEquals(102, originalData.get(2));
  }
}
