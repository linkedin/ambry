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
    Throwable ex = TestUtils.invokeAndGetException(compressor, "getAlgorithmNameBinary");
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Error case - String too long.
    compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("0123456789x");
    ex = TestUtils.invokeAndGetException(compressor, "getAlgorithmNameBinary");
    Assert.assertTrue(ex instanceof IllegalArgumentException);
  }

  @Test
  public void testGetMaxCompressedBufferSize() throws Throwable {
    BaseCompression compressor = Mockito.mock(BaseCompression.class);
    Mockito.when(compressor.estimateMaxCompressedDataSize(Mockito.anyInt())).thenReturn(100);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("ABCDE");
    // The expected buffer size 4 + 6 + 100 = 110.
    int bufferSize = (int) MethodUtils.invokeMethod(compressor, true, "getMaxCompressedBufferSize", 10);
    Assert.assertEquals(110, bufferSize);
  }

  @Test
  public void testCompress() {
    BaseCompression compressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);

    // Test: Compress empty buffer should throw.
    Throwable ex = TestUtils.invokeAndGetException(() -> compressor.compress(null));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Valid case - compressed data is simply a prefix plus the source data.
    Mockito.when(compressor.estimateMaxCompressedDataSize(Mockito.anyInt())).thenReturn(100);
    Mockito.when(compressor.getAlgorithmName()).thenReturn("ABCDE");
    Mockito.when(compressor.compress(Mockito.any(byte[].class), Mockito.any(byte[].class), Mockito.anyInt()))
        .thenAnswer(invocation -> {
          // The abstract compress() method signature:
          // int compress(byte[] sourceData, byte[] compressedBuffer, int compressedBufferOffset);
          byte[] sourceBuffer = invocation.getArgument(0);
          byte[] targetBuffer = invocation.getArgument(1);
          int offset = invocation.getArgument(2);

          // In this test, just copy sourceBuffer to compressedBuffer.
          System.arraycopy(sourceBuffer, 0, targetBuffer, offset, sourceBuffer.length);
          return sourceBuffer.length;
        });

    byte[] testSourceData = new byte[] { 100, 101, 102 };
    Pair<Integer, byte[]> compressResult = compressor.compress(testSourceData);
    // Make sure the format is
    // - 1 byte name size
    // - N bytes for name (5 bytes in this case)
    // - 4 bytes original size (testSourceData size)
    // - X bytes of compressed data (3 bytes in this case).
    Assert.assertEquals((Integer)13, compressResult.getFirst());
    byte[] compressedBuffer = compressResult.getSecond();
    Assert.assertEquals(5, compressedBuffer[0]);
    Assert.assertEquals(3, compressedBuffer[6]);
    Assert.assertEquals(0, compressedBuffer[7]);
    Assert.assertEquals(0, compressedBuffer[8]);
    Assert.assertEquals(0, compressedBuffer[9]);
    Assert.assertEquals(testSourceData[0], compressedBuffer[10]);
    Assert.assertEquals(testSourceData[1], compressedBuffer[11]);
    Assert.assertEquals(testSourceData[2], compressedBuffer[12]);
  }

  @Test
  public void testDecompress() {
    BaseCompression decompressor = Mockito.mock(BaseCompression.class, Mockito.CALLS_REAL_METHODS);

    // Test: decompress empty buffer.
    Throwable ex = TestUtils.invokeAndGetException(() -> decompressor.decompress(null));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    //  Note the compressed buffer format is:
    // - 1 byte name size
    // - N bytes for name (5 bytes in this case)
    // - 4 bytes original size (testSourceData size)
    // - X bytes of compressed data (6 bytes in this case).

    // Test: decompress an invalid buffer that is too small.  Name size is 10, but only 2 bytes long.
    ex = TestUtils.invokeAndGetException(() -> decompressor.decompress(new byte[] { 10, 66 }));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Another invalid buffer case - Name is full, but original size incomplete.
    ex = TestUtils.invokeAndGetException(() -> decompressor.decompress(new byte[] { 2, 66, 67, 3, 0 , 0}));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Valid case.
    Mockito.doAnswer(invocation -> {
      // The abstract decompress() method signature:
      // void decompress(byte[] compressedBuffer, int compressedBufferOffset, byte[] sourceDataBuffer);
      byte[] compressedData = invocation.getArgument(0);
      int offset = invocation.getArgument(1);
      byte[] sourceBuffer = invocation.getArgument(2);

      // In this test, just copy compressed data to sourceBuffer.
      System.arraycopy(compressedData, offset, sourceBuffer, 0, sourceBuffer.length);
      return null;
    }).when(decompressor).decompress(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.any(byte[].class));

    byte[] compressedBuffer = new byte[] { 2, 66, 67, 3, 0, 0, 0, 100, 101, 102};
    byte[] originalData = decompressor.decompress(compressedBuffer);
    Assert.assertEquals(3, originalData.length);
    Assert.assertEquals(100, originalData[0]);
    Assert.assertEquals(101, originalData[1]);
    Assert.assertEquals(102, originalData[2]);
  }
}