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

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
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
    Assert.assertEquals(2, binary.length);
    Assert.assertEquals(1, binary[0]);
    Assert.assertEquals((int) 'A', binary[1]);

    // Test: Large 10 char algorithm name.
    compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("0123456789");
    binary = (byte[]) MethodUtils.invokeMethod(compressor, true, "getAlgorithmNameBinary");
    Assert.assertEquals(11, binary.length);
    Assert.assertEquals(10, binary[0]);
    Assert.assertEquals((int) '0', binary[1]);
    Assert.assertEquals((int) '9', binary[10]);

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
    compressedBuffer[3] = 3;  // original size.
    compressedBuffer[4] = 0;
    compressedBuffer[5] = 0;
    compressedBuffer[6] = 0;
    compressedBuffer[7] = 1;   // compressed data

    BaseCompression decompressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);

    // Test: Invalid version in compressed buffer.
    Exception ex = TestUtils.getException(() -> decompressor.getDecompressBufferSize(compressedBuffer));
    Assert.assertTrue(ex instanceof CompressionException);

    // Test: Valid compressed buffer.
    compressedBuffer[0] = 1;  // Fix version
    int originalSize = decompressor.getDecompressBufferSize(compressedBuffer);
    Assert.assertEquals(3, originalSize);

    originalSize = decompressor.getDecompressBufferSize(compressedBuffer, 0, compressedBuffer.length);
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

    ex = TestUtils.getException(() -> compressor.compress(null, 0, 10));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Invalid source buffer offset (10) in buffer size 10.  Should throw.
    ex = TestUtils.getException(() -> compressor.compress(new byte[10], 10, 10));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Invalid source buffer size (11) in buffer size 10.  Should throw.
    ex = TestUtils.getException(() -> compressor.compress(new byte[10], 0, 11));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Compressed buffer size of 4 is too small.
    ex = TestUtils.getException(() -> compressor.compress(new byte[10], 0, 10,
        new byte[4], 0, 4));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Compressed buffer size invalid buffer parameters.
    ex = TestUtils.getException(() -> compressor.compress(null, 0, 10,
        new byte[4], 0, 4));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Valid case - compressed data is simply a prefix plus the source data.
    Mockito.when(compressor.compressNative(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
            Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
        .thenAnswer(invocation -> {
          byte[] sourceBuffer = invocation.getArgument(0);
          int sourceBufferOffset = invocation.getArgument(1);
          int sourceBufferSize = invocation.getArgument(2);
          byte[] targetBuffer = invocation.getArgument(3);
          int targetBufferOffset = invocation.getArgument(4);

          // In this test, just copy sourceBuffer to compressedBuffer.
          System.arraycopy(sourceBuffer, sourceBufferOffset, targetBuffer, targetBufferOffset, sourceBufferSize);
          return sourceBufferSize;
        });

    byte[] testSourceData = new byte[] { 100, 101, 102 };
    Pair<Integer, byte[]> compressResult = compressor.compress(testSourceData);
    // Make sure the format is
    // - 1 byte version.
    // - 1 byte name size
    // - N bytes for name (4 bytes in this case)
    // - 4 bytes original size (testSourceData size)
    // - X bytes of compressed data (3 bytes in this case).
    Assert.assertEquals((Integer)13, compressResult.getFirst());
    byte[] compressedBuffer = compressResult.getSecond();
    Assert.assertEquals(110, compressedBuffer.length);  //100 + 6 bytes overhead + 4 bytes name.
    Assert.assertEquals(1, compressedBuffer[0]);
    Assert.assertEquals(4, compressedBuffer[1]);
    Assert.assertEquals(3, compressedBuffer[6]);
    Assert.assertEquals(0, compressedBuffer[7]);
    Assert.assertEquals(0, compressedBuffer[8]);
    Assert.assertEquals(0, compressedBuffer[9]);
    Assert.assertEquals(testSourceData[0], compressedBuffer[10]);
    Assert.assertEquals(testSourceData[1], compressedBuffer[11]);
    Assert.assertEquals(testSourceData[2], compressedBuffer[12]);
  }

  @Test
  public void testDecompress() throws CompressionException {
    BaseCompression decompressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);

    // Test: decompress empty buffer.
    Throwable ex = TestUtils.getException(() -> decompressor.decompress(null));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    ex = TestUtils.getException(() -> decompressor.decompress(null, 0, 10));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Invalid compressed buffer offset (10) in buffer size 10.  Should throw.
    ex = TestUtils.getException(() -> decompressor.decompress(new byte[10], 10, 10));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Invalid compression buffer size (11) in buffer size 10.  Should throw.
    ex = TestUtils.getException(() -> decompressor.decompress(new byte[10], 0, 11));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    //  Note the compressed buffer format is:
    // - 1 byte version
    // - 1 byte name size
    // - N bytes for name (5 bytes in this case)
    // - 4 bytes original size (testSourceData size)
    // - X bytes of compressed data (6 bytes in this case).

    // Test: decompress an invalid buffer that is too small.  Version is 1, Name size is 10, but only 2 bytes long.
    ex = TestUtils.getException(() -> decompressor.decompress(new byte[] { 1, 10, 66 }));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Another invalid buffer case - Version is 1, Name size is 2, 2-char name, but original size incomplete.
    ex = TestUtils.getException(() -> decompressor.decompress(new byte[] { 1, 2, 66, 67, 3, 0 , 0}));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: decompress a valid compressed buffer, but source data buffer size (2) is too small.  Original size = 3.
    // Version is 1, name length is 2, 2-char name, size is 3, 3 bytes of compressed data.
    final byte[] compressedBuffer = new byte[] { 1, 2, 66, 67, 3, 0, 0, 0, 100, 101, 102};
    ex = TestUtils.getException(() -> decompressor.decompress(
        compressedBuffer, 0, compressedBuffer.length, new byte[2], 0, 2));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Compressed buffer size invalid buffer parameters.
    ex = TestUtils.getException(() -> decompressor.decompress(null, 0, 10,
        new byte[4], 0, 4));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Valid case.
    Mockito.doAnswer(invocation -> {
      byte[] compressedData = invocation.getArgument(0);
      int compressedDataOffset = invocation.getArgument(1);
      int compressedDataSize = invocation.getArgument(2);
      byte[] sourceBuffer = invocation.getArgument(3);
      int sourceBufferOffset = invocation.getArgument(4);

      // In this test, just copy compressed data to sourceBuffer.
      System.arraycopy(compressedData, compressedDataOffset,
          sourceBuffer, sourceBufferOffset, compressedDataSize);
      return null;
    }).when(decompressor).decompressNative(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

    byte[] originalData = decompressor.decompress(compressedBuffer);
    Assert.assertEquals(3, originalData.length);
    Assert.assertEquals(100, originalData[0]);
    Assert.assertEquals(101, originalData[1]);
    Assert.assertEquals(102, originalData[2]);
  }
}
