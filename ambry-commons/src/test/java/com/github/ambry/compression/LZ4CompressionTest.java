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
import java.nio.charset.StandardCharsets;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore("javax.management.*")
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
  public void testCompressAndDecompressNative_MinimumLevel() throws CompressionException {
    LZ4Compression compression = new LZ4Compression();
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    compressAndDecompressNativeTest(compression, "Test my minimum compression message using minimum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    compressAndDecompressNativeTest(compression, "Test my minimum compression message using minimum level.", 2, 3);
  }

  @Test
  public void testCompressAndDecompress_MinimumLevel() throws CompressionException {
    LZ4Compression compression = new LZ4Compression();
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    // Test compression API with dedicated buffer (without extra bytes on left or right of buffer).
    compressAndDecompressTest(compression, "Test my minimum compression message using minimum level.", 0 , 0);

    // Test compression API with mid-buffer (with extra bytes on left and right of buffer.)
    compressAndDecompressTest(compression, "Test my minimum compression message using minimum level.", 2 , 3);
  }

  @Test
  public void testCompressAndDecompressNative_MaximumLevel() throws CompressionException {
    LZ4Compression compression = new LZ4Compression();
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    compressAndDecompressNativeTest(compression, "Test my maximum compression message using maximum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    compressAndDecompressNativeTest(compression, "Test my maximum compression message using maximum level.", 2, 1);
  }

  @Test
  public void testCompressAndDecompress_MaximumLevel() throws CompressionException {
    LZ4Compression compression = new LZ4Compression();
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    // Test compression API with dedicated buffer (without extra bytes on left or right of buffer).
    compressAndDecompressTest(compression, "Test my maximum compression message using maximum level.", 0, 0);

    // Test compression API with mid-buffer (with extra bytes on left and right of buffer.)
    compressAndDecompressTest(compression, "Test my maximum compression message using maximum level.", 2, 1);
  }

  @Test
  public void testCompressNativeFailed() throws Exception {
    // Create a compressor that throws when calling
    // compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen)
    LZ4Compressor throwCompressor = Mockito.mock(LZ4Compressor.class, Mockito.CALLS_REAL_METHODS);
    Exception theException = new RuntimeException("test");
    Mockito.when(throwCompressor.compress(Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt(),
      Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt())).thenThrow(theException);

    // Mock getCompressor() to return a compressor that throws when compress() is called.
    LZ4Compression lz4 = PowerMockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(lz4, "getCompressor").thenReturn(throwCompressor);
    Exception ex = TestUtils.getException(() ->
        lz4.compressNative(ByteBuffer.wrap("ABC".getBytes(StandardCharsets.UTF_8)), 0, 3,
            ByteBuffer.wrap(new byte[10]), 0, 10));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(theException, ex.getCause());
  }

  @Test
  public void testDecompressNativeFailed() throws Exception {

    // Create a compressor that throws when calling
    // decompress(byte[] src, int srcOff, byte[] dest, int destOff, int destLen)
    LZ4FastDecompressor throwDecompressor = Mockito.mock(LZ4FastDecompressor.class, Mockito.CALLS_REAL_METHODS);
    Exception theException = new RuntimeException("failed");
    Mockito.when(throwDecompressor.decompress(Mockito.any(ByteBuffer.class), Mockito.anyInt(),
        Mockito.any(ByteBuffer.class), Mockito.anyInt(), Mockito.anyInt())).thenThrow(theException);

    // Mock getDecompressor() to return a decompressor that throws when decompress() is called.
    LZ4Compression lz4 = PowerMockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(lz4, "getDecompressor").thenReturn(throwDecompressor);
    Exception ex = TestUtils.getException(() ->
        lz4.decompressNative(ByteBuffer.wrap(new byte[10]), 0, 10, ByteBuffer.wrap(new byte[10]), 0, 10));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(theException, ex.getCause());
  }

  /**
   * Test compression using compressNative() and decompressNative() APIs.
   * It does not use the structural formatting introduced by compress().
   */
  public static void compressAndDecompressNativeTest(BaseCompression compression, String testMessage,
      int bufferLeftPadSize, int bufferRightPadSize) throws CompressionException {
    // Apply compression to testMessage.
    // compressedBuffer consists of bytes[bufferLeftPadSize] + bytes[actualCompressedSize]
    // + bytes[compressUnusedSpace] + bytes[bufferRightPadSize]
    ByteBuffer sourceBuffer = ByteBuffer.wrap(testMessage.getBytes(StandardCharsets.UTF_8));
    int sourceBufferSize = sourceBuffer.remaining();

    int compressedBufferSize = compression.estimateMaxCompressedDataSize(sourceBufferSize);
    ByteBuffer compressedBuffer = ByteBuffer.wrap(new byte[bufferLeftPadSize + compressedBufferSize + bufferRightPadSize]);
    int actualCompressedSize = compression.compressNative(sourceBuffer, 0, sourceBufferSize,
        compressedBuffer, bufferLeftPadSize, compressedBufferSize);
    Assert.assertTrue(actualCompressedSize > 0);

    // Apply decompression.
    // compressedBuffer has bytes[bufferLeftPadSize] + bytes[actualCompressedSize] + bytes[bufferRightPadSize]
    // decompressedBuffer contains bytes[bufferLeftPadSize] + bytes[sourceBufferSize] + bytes[bufferRightPadSize]
    ByteBuffer decompressedBuffer = ByteBuffer.wrap(new byte[bufferLeftPadSize + sourceBufferSize + bufferRightPadSize]);
    compression.decompressNative(compressedBuffer, bufferLeftPadSize, actualCompressedSize,
        decompressedBuffer, bufferLeftPadSize, sourceBufferSize);

    String decompressedMessage = new String(decompressedBuffer.array(), bufferLeftPadSize, sourceBufferSize,
        StandardCharsets.UTF_8);
    Assert.assertEquals(testMessage, decompressedMessage);
  }

  /**
   * Test compression using compress() and decompress() APIs that uses an internal structure.
   * It tests a combination of heap buffers and direct memory as input and output.
   */
  public static void compressAndDecompressTest(Compression compression, String testMessage,
      int bufferLeftPadSize, int bufferRightPadSize) throws CompressionException {

    byte[] testMessageBinary = testMessage.getBytes(StandardCharsets.UTF_8);
    int testMessageSize = testMessageBinary.length;
    int compressedBufferSize = compression.getCompressBufferSize(testMessageSize);
    int paddedCompressedBufferSize = bufferLeftPadSize + compressedBufferSize + bufferRightPadSize;

    // Apply compression to testMessage using Heap and Direct memory.
    // compressedBuffer consists of bytes[bufferLeftPadSize] + bytes[actualCompressedSize] + bytes[bufferRightPadSize]
    ByteBuffer sourceBufferHeap = ByteBuffer.wrap(testMessageBinary);
    ByteBuffer sourceBufferDirect = ByteBuffer.allocateDirect(testMessageBinary.length);
    sourceBufferDirect.put(testMessageBinary);
    ByteBuffer compressedBufferHeap = ByteBuffer.allocate(paddedCompressedBufferSize);
    ByteBuffer compressedBufferDirect = ByteBuffer.allocateDirect(paddedCompressedBufferSize);

    // Compress Heap source to Heap output.
    sourceBufferHeap.position(0);
    compressedBufferHeap.position(bufferLeftPadSize);
    int actualCompressedSize = compression.compress(sourceBufferHeap, compressedBufferHeap);
    Assert.assertTrue(actualCompressedSize > 0);
    Assert.assertEquals(0, sourceBufferHeap.remaining());
    Assert.assertEquals(testMessageSize, sourceBufferHeap.position());
    Assert.assertEquals(actualCompressedSize, compressedBufferHeap.position() - bufferLeftPadSize);

    int nextActualCompressedSize;
    if (!compression.requireMatchingBufferType()) {
      // Compress Heap source to Direct output.
      sourceBufferHeap.position(0);
      compressedBufferDirect.position(bufferLeftPadSize);
      nextActualCompressedSize = compression.compress(sourceBufferHeap, compressedBufferDirect);
      Assert.assertEquals(actualCompressedSize, nextActualCompressedSize);
      Assert.assertTrue(nextActualCompressedSize > 0);
      Assert.assertEquals(0, sourceBufferHeap.remaining());
      Assert.assertEquals(testMessageSize, sourceBufferHeap.position());
      Assert.assertEquals(nextActualCompressedSize, compressedBufferDirect.position() - bufferLeftPadSize);

      // Compress Direct source to Heap output.
      sourceBufferDirect.position(0);
      compressedBufferHeap.position(bufferLeftPadSize);
      nextActualCompressedSize = compression.compress(sourceBufferDirect, compressedBufferHeap);
      Assert.assertEquals(actualCompressedSize, nextActualCompressedSize);
      Assert.assertTrue(nextActualCompressedSize > 0);
      Assert.assertEquals(0, sourceBufferDirect.remaining());
      Assert.assertEquals(testMessageSize, sourceBufferDirect.position());
      Assert.assertEquals(actualCompressedSize, compressedBufferHeap.position() - bufferLeftPadSize);
    }

    // Compress Direct source to Direct output.
    sourceBufferDirect.position(0);
    compressedBufferDirect.position(bufferLeftPadSize);
    nextActualCompressedSize = compression.compress(sourceBufferDirect, compressedBufferDirect);
    Assert.assertEquals(actualCompressedSize, nextActualCompressedSize);
    Assert.assertTrue(nextActualCompressedSize > 0);
    Assert.assertEquals(0, sourceBufferDirect.remaining());
    Assert.assertEquals(testMessageSize, sourceBufferDirect.position());
    Assert.assertEquals(actualCompressedSize, compressedBufferDirect.position() - bufferLeftPadSize);

    // Apply decompression.
    // compressedBuffer has bytes[bufferLeftPadSize] + bytes[actualCompressedSize] + bytes[bufferRightPadSize]
    // decompressedBuffer contains bytes[bufferLeftPadSize] + bytes[sourceBufferSize] + bytes[bufferRightPadSize]
    compressedBufferHeap.limit(bufferLeftPadSize + actualCompressedSize);
    compressedBufferDirect.limit(bufferLeftPadSize + actualCompressedSize);
    int paddedDecompressedBufferSize = bufferLeftPadSize + testMessageSize + bufferRightPadSize;
    ByteBuffer decompressedBufferHeap = ByteBuffer.allocate(paddedDecompressedBufferSize);
    ByteBuffer decompressedBufferDirect = ByteBuffer.allocateDirect(paddedDecompressedBufferSize);
    byte[] decompressedMessage = new byte[testMessageSize];

    // Decompress Heap to Heap.
    compressedBufferHeap.position(bufferLeftPadSize);
    decompressedBufferHeap.position(bufferLeftPadSize);
    int decompressedSize = compression.decompress(compressedBufferHeap, decompressedBufferHeap);
    Assert.assertEquals(testMessageSize, decompressedSize);
    Assert.assertEquals(testMessageSize, decompressedBufferHeap.position() - bufferLeftPadSize);
    Assert.assertEquals(0, compressedBufferHeap.remaining());
    Assert.assertEquals(testMessage, new String(decompressedBufferHeap.array(), bufferLeftPadSize, testMessageSize, StandardCharsets.UTF_8));

    if (!compression.requireMatchingBufferType()) {
      // Decompress Heap to Direct.
      compressedBufferHeap.position(bufferLeftPadSize);
      decompressedBufferDirect.position(bufferLeftPadSize);
      decompressedSize = compression.decompress(compressedBufferHeap, decompressedBufferDirect);
      Assert.assertEquals(testMessageSize, decompressedSize);
      Assert.assertEquals(testMessageSize, decompressedBufferDirect.position() - bufferLeftPadSize);
      Assert.assertEquals(0, compressedBufferHeap.remaining());
      decompressedBufferDirect.position(bufferLeftPadSize).get(decompressedMessage);
      Assert.assertEquals(testMessage, new String(decompressedMessage, StandardCharsets.UTF_8));

      // Decompress Direct to Heap.
      compressedBufferDirect.position(bufferLeftPadSize);
      decompressedBufferHeap.position(bufferLeftPadSize);
      decompressedSize = compression.decompress(compressedBufferDirect, decompressedBufferHeap);
      Assert.assertEquals(testMessageSize, decompressedSize);
      Assert.assertEquals(testMessageSize, decompressedBufferHeap.position() - bufferLeftPadSize);
      Assert.assertEquals(0, compressedBufferDirect.remaining());
      Assert.assertEquals(testMessage, new String(decompressedBufferHeap.array(), bufferLeftPadSize, testMessageSize, StandardCharsets.UTF_8));
    }

    // Decompress Direct to Direct.
    compressedBufferDirect.position(bufferLeftPadSize);
    decompressedBufferDirect.position(bufferLeftPadSize);
    decompressedSize = compression.decompress(compressedBufferDirect, decompressedBufferDirect);
    Assert.assertEquals(testMessageSize, decompressedSize);
    Assert.assertEquals(testMessageSize, decompressedBufferDirect.position() - bufferLeftPadSize);
    Assert.assertEquals(0, compressedBufferDirect.remaining());
    decompressedBufferDirect.position(bufferLeftPadSize).get(decompressedMessage);
    Assert.assertEquals(testMessage, new String(decompressedMessage, StandardCharsets.UTF_8));
  }
}
