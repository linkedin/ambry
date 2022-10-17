/*
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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.compression.CompressionException;
import com.github.ambry.compression.LZ4Compression;
import com.github.ambry.compression.ZstdCompression;
import com.github.ambry.config.CompressionConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class CompressionServiceTest {

  @Test
  public void testIsBlobCompressible() {
    BlobProperties blobProperties = new BlobProperties(1234L, "testServiceID", (short) 2222, (short) 3333, true);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);

    // Test: compression disabled.  Compression does nothing.
    service.isCompressionEnabled = false;
    Assert.assertFalse(service.isBlobCompressible(blobProperties));
    service.isCompressionEnabled = true;

    // Test: Disable compression due to content-ending.
    service.isSkipWithContentEncoding = true;
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "contentType", true, 12345L, (short) 111, (short) 222, true,
            "testTag", "testEncoding", "testFileName");
    service.isSkipWithContentEncoding = true;
    Assert.assertFalse(service.isBlobCompressible(blobProperties));
    Assert.assertEquals(1, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkipContentEncoding.getCount());
    service.isSkipWithContentEncoding = false;

    // Test: Disable compression due to content-type (image/* is blocked, text/* is allowed)
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "image/123", true, 12345L, (short) 111, (short) 222, true,
            "testTag", null, "testFileName");
    Assert.assertFalse(service.isBlobCompressible(blobProperties));
    Assert.assertEquals(2, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkipContentTypeFiltering.getCount());

    // Test: success case.
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "text/123", true, 12345L, (short) 111, (short) 222, true,
        "testTag", null, "testFileName");
    Assert.assertTrue(service.isBlobCompressible(blobProperties));
  }

  @Test
  public void testCompressBuffer() {
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);
    byte[] sourceBuffer = "Test Message for testing purpose.  The Message is part of the testing message.".getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);

    // Test: Disable compression due to data size too small.
    service.isCompressionEnabled = true;
    service.minimalSourceDataSizeInBytes = 1000;
    ByteBuf compressedBuffer = service.compressChunk(sourceByteBuf, true);
    Assert.assertNull(compressedBuffer);
    Assert.assertEquals(1, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkipSizeTooSmall.getCount());
    service.minimalSourceDataSizeInBytes = 1;

    // Test: Disable compression due to byte conversion failed.
    CompressionService mockedService = Mockito.spy(service);
    Mockito.doThrow(new RuntimeException("Failed.")).when(mockedService).convertByteBufToByteArray(Mockito.any());
    compressedBuffer = mockedService.compressChunk(sourceByteBuf, true);
    Assert.assertNull(compressedBuffer);
    Assert.assertEquals(1, metrics.compressErrorRate.getCount());
    Assert.assertEquals(1, metrics.compressErrorBufferConversion.getCount());
  }

  @Test
  public void testCompressThrow() throws CompressionException {
    byte[] sourceBuffer = "Test Message for testing purpose.  The Message is part of the testing message.".getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());

    // Test: Disable compression due to compression failed.
    // Create a test CompressionService and replace the compressor with a mock.
    CompressionService compressionService = new CompressionService(null, metrics);
    compressionService.minimalSourceDataSizeInBytes = 1;
    LZ4Compression mockCompression = Mockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    Mockito.doThrow(new RuntimeException("Compress failed.")).when(mockCompression).compress(
        Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
    compressionService.defaultCompressor = mockCompression;
    ByteBuf compressedBuffer = compressionService.compressChunk(sourceByteBuf, true);
    Assert.assertNull(compressedBuffer);
    Assert.assertEquals(1, metrics.compressErrorRate.getCount());
    Assert.assertEquals(1, metrics.compressErrorCompressFailed.getCount());
  }

  @Test
  public void testCompressWithLowRatio() throws CompressionException {
    byte[] sourceBuffer = "Test Message for testing purpose.  The Message is part of the testing message.".getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService compressionService = new CompressionService(null, metrics);
    compressionService.minimalSourceDataSizeInBytes = 1;

    // Test: Compression skipped because compression ratio is too low.
    LZ4Compression mockCompression = Mockito.mock(LZ4Compression.class);
    Mockito.when(mockCompression.compress(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(new Pair<>(sourceBuffer.length, sourceBuffer));
    compressionService.allCompressions.put(LZ4Compression.ALGORITHM_NAME, mockCompression);
    ByteBuf compressedBuffer = compressionService.compressChunk(sourceByteBuf, true);
    Assert.assertNull(compressedBuffer);
    Assert.assertEquals(1, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkipRatioTooSmall.getCount());
  }

  @Test
  public void testCompressSucceeded() {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message." +
        "Test Message for testing purpose.  The Message is part of the testing message." +
        "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());

    // Test: Happy case - compression succeed partial full.
    CompressionService service = new CompressionService(null, metrics);
    service.isCompressionEnabled = true;
    service.minimalCompressRatio = 1.0;
    service.minimalSourceDataSizeInBytes = 1;

    ByteBuf compressedBuffer = service.compressChunk(sourceByteBuf, false);
    Assert.assertNotEquals(sourceByteBuf.readableBytes(), compressedBuffer.readableBytes());
    Assert.assertEquals(1, metrics.compressAcceptRate.getCount());

    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics(ZstdCompression.ALGORITHM_NAME);
    Assert.assertTrue(algorithmMetrics.compressRatioPercent.getCount() > 0);
    Assert.assertEquals(1, algorithmMetrics.compressRate.getCount());
    Assert.assertTrue(algorithmMetrics.smallSizeCompressTimeInMicroseconds.getCount() > 0);
    Assert.assertTrue(algorithmMetrics.smallSizeCompressSpeedMBPerSec.getCount() > 0);

    // Happy case - compression succeed full chunk.
    service.defaultCompressor = service.allCompressions.get(LZ4Compression.ALGORITHM_NAME);
    Assert.assertEquals(LZ4Compression.ALGORITHM_NAME, service.defaultCompressor.getAlgorithmName());
    compressedBuffer = service.compressChunk(sourceByteBuf, true);
    Assert.assertNotEquals(sourceByteBuf.readableBytes(), compressedBuffer.readableBytes());
    Assert.assertEquals(2, metrics.compressAcceptRate.getCount());

    algorithmMetrics = metrics.getAlgorithmMetrics(LZ4Compression.ALGORITHM_NAME);
    Assert.assertTrue(algorithmMetrics.compressRatioPercent.getCount() > 0);
    Assert.assertEquals(1, algorithmMetrics.compressRate.getCount());
    Assert.assertTrue(algorithmMetrics.fullSizeCompressTimeInMicroseconds.getCount() > 0);
    Assert.assertTrue(algorithmMetrics.fullSizeCompressSpeedMBPerSec.getCount() > 0);
  }

  @Test
  public void testDecompressWithConversionFailed() {
    // Test: Decompress failed due to byte conversion throws.
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();

    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);

    // Test: Invalid parameter
    Exception ex = TestUtils.getException(() -> service.decompress(null, sourceBuffer.length));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Test full-chunk.
    // Test: Decompression failed due to byte conversion failed.
    CompressionService mockedService = Mockito.spy(service);
    Mockito.doThrow(new RuntimeException("Failed.")).when(mockedService).convertByteBufToByteArray(Mockito.any());
    ex = TestUtils.getException(() -> mockedService.decompress(sourceByteBuf, sourceBuffer.length));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(1, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressErrorBufferConversion.getCount());
  }

  @Test
  public void testDecompressWithInvalidBuffer() {
    // Test: Decompress failed due to byte conversion throws.
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);

    // Test: buffer does not contain algorithm name. (name size is 100);
    final byte[] badCompressedBuffer = new byte[]{1, 100, 1, 2, 3, 4, 5};
    Exception ex = TestUtils.getException(
        () -> service.decompress(Unpooled.wrappedBuffer(badCompressedBuffer), badCompressedBuffer.length));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(1, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressErrorBufferTooSmall.getCount());

    // Test: algorithm name does not exist.
    final byte[] badCompressedBuffer2 = new byte[]{1, 4, 65, 66, 67, 68, 5, 1, 2, 3, 4, 5};
    ex = TestUtils.getException(
        () -> service.decompress(Unpooled.wrappedBuffer(badCompressedBuffer2), badCompressedBuffer2.length));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(2, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressErrorUnknownAlgorithmName.getCount());
  }

  @Test
  public void testDecompressWithAlgorithmThrows() throws CompressionException {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    Pair<Integer, byte[]> compressedInfo = new LZ4Compression().compress(sourceBuffer);
    ByteBuf compressedBuffer = Unpooled.wrappedBuffer(compressedInfo.getSecond());

    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);

    // Test: decompress() throws exception.
    // Create a test CompressionService and replace the compressor with a mock.
    LZ4Compression mockCompression = Mockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    Mockito.doThrow(new RuntimeException("Decompress failed.")).when(mockCompression).decompress(
        Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
    service.allCompressions.add(mockCompression);
    Exception ex = TestUtils.getException(() -> service.decompress(compressedBuffer, compressedBuffer.readableBytes()));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(1, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressErrorDecompressFailed.getCount());
  }

  @Test
  public void testDecompressSucceeded() throws CompressionException {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    Pair<Integer, byte[]> compressedInfo = new LZ4Compression().compress(sourceBuffer);
    ByteBuf compressedBuffer = Unpooled.wrappedBuffer(compressedInfo.getSecond(), 0, compressedInfo.getFirst());

    // Decompressed full chunk.
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);
    ByteBuf decompressedBuffer = service.decompress(compressedBuffer, sourceBuffer.length);
    Assert.assertEquals(decompressedBuffer.readableBytes(), sourceBuffer.length);
    Assert.assertEquals(1, metrics.decompressSuccessRate.getCount());
    Assert.assertTrue(metrics.decompressExpandSizeBytes.getCount() > 0);

    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics(LZ4Compression.ALGORITHM_NAME);
    Assert.assertEquals(1, algorithmMetrics.decompressRate.getCount());
    Assert.assertTrue(algorithmMetrics.fullSizeDecompressTimeInMicroseconds.getCount() > 0);

    // Decompress partial chunk.
    decompressedBuffer = service.decompress(compressedBuffer, sourceBuffer.length - 1);
    Assert.assertEquals(decompressedBuffer.readableBytes(), sourceBuffer.length);
    Assert.assertEquals(2, metrics.decompressSuccessRate.getCount());
    Assert.assertTrue(metrics.decompressExpandSizeBytes.getCount() > 0);

    algorithmMetrics = metrics.getAlgorithmMetrics(LZ4Compression.ALGORITHM_NAME);
    Assert.assertEquals(2, algorithmMetrics.decompressRate.getCount());
    Assert.assertTrue(algorithmMetrics.smallSizeDecompressTimeInMicroseconds.getCount() > 0);
  }

  @Test
  public void testConvertByteBufToByteArray() {
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);

    // Test: Convert wrapped byte[] buffer to byte[]
    byte[] arrayBuffer = new byte[] { -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    ByteBuf sourceBuffer = Unpooled.wrappedBuffer(arrayBuffer, 3, 8);
    Triple<byte[], Integer, Integer> bufferInfo = service.convertByteBufToByteArray(sourceBuffer);
    Assert.assertEquals(arrayBuffer, bufferInfo.getLeft());
    Assert.assertEquals(3, (int) bufferInfo.getMiddle());
    Assert.assertEquals(8, (int) bufferInfo.getRight());

    // Test: Convert wrapped ByteBuffer buffer to byte[]
    sourceBuffer = Unpooled.wrappedBuffer(ByteBuffer.wrap(arrayBuffer, 3, 8));
    bufferInfo = service.convertByteBufToByteArray(sourceBuffer);
    Assert.assertEquals(arrayBuffer, bufferInfo.getLeft());
    Assert.assertEquals(3, (int) bufferInfo.getMiddle());
    Assert.assertEquals(8, (int) bufferInfo.getRight());

    // Test: Convert multiple wrapped buffers to byte[].
    byte[] arrayBuffer1 = new byte[] { -2, -1, 0, };
    byte[] arrayBuffer2 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    sourceBuffer = Unpooled.wrappedBuffer(arrayBuffer1, arrayBuffer2);
    bufferInfo = service.convertByteBufToByteArray(sourceBuffer);
    Assert.assertNotEquals(arrayBuffer, bufferInfo.getLeft());
    Assert.assertEquals(0, (int) bufferInfo.getMiddle());
    Assert.assertEquals(12, (int) bufferInfo.getRight());
  }

  @Test
  public void testCompressThenDecompress() throws CompressionException {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();

    // Decompressed full chunk.  Note: algorithm is not specified in config.  It would use the Zstd as default.
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(null, metrics);
    service.minimalSourceDataSizeInBytes = 1;
    service.minimalCompressRatio = 1.0;

    // Compress the source buffer.
    ByteBuf compressedBuffer = service.compressChunk(Unpooled.wrappedBuffer(sourceBuffer), false);
    Assert.assertNotEquals(sourceBuffer.length, compressedBuffer.readableBytes());
    Assert.assertEquals(1, metrics.compressAcceptRate.getCount());

    // Decompress the source buffer.
    ByteBuf decompressedBuffer = service.decompress(compressedBuffer, sourceBuffer.length);
    Assert.assertEquals(sourceBuffer.length, decompressedBuffer.readableBytes());
    Assert.assertEquals(1, metrics.decompressSuccessRate.getCount());
    Assert.assertTrue(metrics.decompressExpandSizeBytes.getCount() > 0);
    Assert.assertArrayEquals(decompressedBuffer.array(), sourceBuffer);

    // Zstd is the default compression algorithm.
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics(ZstdCompression.ALGORITHM_NAME);
    Assert.assertEquals(1, algorithmMetrics.decompressRate.getCount());
    Assert.assertTrue(algorithmMetrics.fullSizeDecompressTimeInMicroseconds.getCount() > 0);
  }

  @Test
  public void testIsCompressibleContentType() {
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    Properties properties = new Properties();
    properties.put("router.compression.compress.content.types", "text/111, text/222, comp1, comp2  , comp3");
    properties.put("router.compression.algorithm.name", "BADNAME");
    CompressionConfig config = new CompressionConfig(new VerifiableProperties(properties));
    CompressionService compressionService = new CompressionService(config, metrics);

    // Test content-type specified in compressible content-type.
    Assert.assertTrue(compressionService.isCompressibleContentType("text/111"));
    Assert.assertTrue(compressionService.isCompressibleContentType("TEXT/222"));
    Assert.assertTrue(compressionService.isCompressibleContentType("Text/222; charset=UTF8"));

    // Test content-type specified in compressible context-type prefix.
    Assert.assertTrue(compressionService.isCompressibleContentType("comp1/111"));
    Assert.assertTrue(compressionService.isCompressibleContentType("Comp2/222"));
    Assert.assertTrue(compressionService.isCompressibleContentType("COMP3/222; charset=UTF8"));

    // Test unknown content-type.
    Assert.assertFalse(compressionService.isCompressibleContentType("image/111"));
    Assert.assertFalse(compressionService.isCompressibleContentType("unknown/111"));
    Assert.assertFalse(compressionService.isCompressibleContentType(""));
  }
}
