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
import java.nio.charset.StandardCharsets;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LZ4Compression.class)
public class LZ4CompressionTest {

  @Test
  public void testGetAlgorithmName() {
    LZ4Compression compression = new LZ4Compression();
    Assert.assertTrue(compression.getAlgorithmName().length() > 0);
    Assert.assertTrue(compression.getAlgorithmName().length() < BaseCompression.MAX_ALGORITHM_NAME_LENGTH);
  }

  @Test
  public void testCompressionLevel() {
    LZ4Compression compression = new LZ4Compression();
    Assert.assertEquals(0, compression.getMinimumCompressionLevel());
    Assert.assertTrue(compression.getMaximumCompressionLevel() > 0);
    Assert.assertTrue(compression.getDefaultCompressionLevel() >= 0);
  }

  @Test
  public void testEstimateMaxCompressedDataSize() {
    LZ4Compression compression = new LZ4Compression();
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(1) > 1);
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(100) > 100);
  }

  @Test
  public void testCompressAndDecompress_MinimumLevel() throws CompressionException {
    LZ4Compression compression = new LZ4Compression();
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    compressAndDecompressNativeTest(compression, "Test my minimum compression message using minimum level.", 0 , 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    compressAndDecompressNativeTest(compression, "Test my minimum compression message using minimum level.", 2 , 3);
  }

  @Test
  public void testCompressAndDecompress_MaximumLevel() throws CompressionException {
    LZ4Compression compression = new LZ4Compression();
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    compressAndDecompressNativeTest(compression, "Test my maximum compression message using maximum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    compressAndDecompressNativeTest(compression, "Test my maximum compression message using maximum level.", 2, 1);
  }

  @Test
  public void testCompressNativeFailed() throws Exception {
    // Create a compressor that throws when calling
    // compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen)
    LZ4Compressor throwCompressor = Mockito.mock(LZ4Compressor.class, Mockito.CALLS_REAL_METHODS);
    Exception theException = new RuntimeException("test");
    Mockito.when(throwCompressor.compress(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
      Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenThrow(theException);

    // Mock getCompressor() to return a compressor that throws when compress() is called.
    LZ4Compression lz4 = PowerMockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(lz4, "getCompressor").thenReturn(throwCompressor);
    Exception ex = TestUtils.getException(() ->
        lz4.compressNative("ABC".getBytes(StandardCharsets.UTF_8), 0, 3,
            new byte[10], 0, 10));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(theException, ex.getCause());
  }

  @Test
  public void testDecompressNativeFailed() throws Exception {

    // Create a compressor that throws when calling
    // decompress(byte[] src, int srcOff, byte[] dest, int destOff, int destLen)
    LZ4FastDecompressor throwDecompressor = Mockito.mock(LZ4FastDecompressor.class, Mockito.CALLS_REAL_METHODS);
    Exception theException = new RuntimeException("failed");
    Mockito.when(throwDecompressor.decompress(Mockito.any(byte[].class), Mockito.anyInt(),
        Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenThrow(theException);

    // Mock getDecompressor() to return a decompressor that throws when decompress() is called.
    LZ4Compression lz4 = PowerMockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(lz4, "getDecompressor").thenReturn(throwDecompressor);
    Exception ex = TestUtils.getException(() ->
        lz4.decompressNative(new byte[10], 0, 10, new byte[10], 0, 10));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(theException, ex.getCause());
  }

  public static void compressAndDecompressNativeTest(BaseCompression compression, String testMessage,
      int bufferLeftPadSize, int bufferRightPadSize) throws CompressionException {
    // Apply compression to testMessage.
    // oversizeCompressedBuffer consists of bytes[bufferLeftPadSize] + bytes[actualCompressedSize]
    // + bytes[compressUnusedSpace] + bytes[bufferRightPadSize]
    byte[] sourceBuffer = testMessage.getBytes(StandardCharsets.UTF_8);
    int estimatedCompressedSize = compression.estimateMaxCompressedDataSize(sourceBuffer.length);
    byte[] oversizeCompressedBuffer = new byte[bufferLeftPadSize + estimatedCompressedSize + bufferRightPadSize];
    int actualCompressedSize = compression.compressNative(sourceBuffer, 0, sourceBuffer.length,
        oversizeCompressedBuffer, bufferLeftPadSize, estimatedCompressedSize);
    Assert.assertTrue(actualCompressedSize > 0);

    // Apply decompression.
    // compressedBuffer has bytes[bufferLeftPadSize] + bytes[actualCompressedSize] + bytes[bufferRightPadSize]
    // decompressedBuffer contains bytes[bufferLeftPadSize] + bytes[sourceBufferSize] + bytes[bufferRightPadSize]
    byte[] compressedBuffer = new byte[bufferLeftPadSize + actualCompressedSize + bufferRightPadSize];
    System.arraycopy(oversizeCompressedBuffer, bufferLeftPadSize, compressedBuffer, bufferLeftPadSize,
        actualCompressedSize);
    byte[] decompressedBuffer = new byte[bufferLeftPadSize + sourceBuffer.length + bufferRightPadSize];
    compression.decompressNative(compressedBuffer, bufferLeftPadSize, actualCompressedSize,
        decompressedBuffer, bufferLeftPadSize, sourceBuffer.length);

    String decompressedMessage = new String(decompressedBuffer, bufferLeftPadSize, sourceBuffer.length,
        StandardCharsets.UTF_8);
    Assert.assertEquals(testMessage, decompressedMessage);
  }
}
