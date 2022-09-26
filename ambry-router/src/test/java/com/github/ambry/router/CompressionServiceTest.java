package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.compression.CompressionException;
import com.github.ambry.compression.LZ4Compression;
import com.github.ambry.compression.ZstdCompression;
import com.github.ambry.config.CompressionConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class CompressionServiceTest {

  @Test
  public void compressSkipDueToConfig() {
    BlobProperties blobProperties = new BlobProperties(1234L, "testServiceID", (short) 2222, (short) 3333, true);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionConfig config = new CompressionConfig();
    CompressionService service = new CompressionService(config, metrics);
    byte[] sourceBuffer = "Test Message for testing purpose.  The Message is part of the testing message.".getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);

    // Test: compression disabled.  Compression does nothing.
    config.isCompressionEnabled = false;
    ByteBuf compressedBuffer = service.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    config.isCompressionEnabled = true;

    // Test: Disable compression due to content-ending.
    config.isSkipWithContentEncoding = true;
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "contentType", true, 12345L, (short) 111, (short) 222, true,
            "testTag", "testEncoding", "testFileName");
    config.isSkipWithContentEncoding = true;
    compressedBuffer = service.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    Assert.assertEquals(1, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkip_ContentEncoding.getCount());
    config.isSkipWithContentEncoding = false;

    // Test: Disable compression due to content-type (image/* is blocked, text/* is allowed)
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "image/123", true, 12345L, (short) 111, (short) 222, true,
            "testTag", null, "testFileName");
    compressedBuffer = service.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    Assert.assertEquals(2, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkip_ContentTypeFiltering.getCount());

    // Test: Disable compression due to data size too small.
    config.minimalSourceDataSizeInBytes = 1000;
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "text/123", true, 12345L, (short) 111, (short) 222, true,
            "testTag", null, "testFileName");
    compressedBuffer = service.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    Assert.assertEquals(3, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkip_SizeTooSmall.getCount());
    config.minimalSourceDataSizeInBytes = 1;

    // Test: Disable compression due to byte conversion failed.
    CompressionService mockedService = Mockito.spy(service);
    Mockito.doThrow(new RuntimeException("Failed.")).when(mockedService).convertByteBufToByteArray(Mockito.any());
    blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "text/123", true, 12345L, (short) 111, (short) 222, true,
            "testTag", null, "testFileName");
    compressedBuffer = mockedService.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    Assert.assertEquals(1, metrics.compressErrorRate.getCount());
    Assert.assertEquals(1, metrics.compressError_BufferConversion.getCount());
  }

  @Test
  public void compressThrow() throws CompressionException {
    CompressionConfig config = new CompressionConfig();
    byte[] sourceBuffer = "Test Message for testing purpose.  The Message is part of the testing message.".getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());

    // Test: Disable compression due to compression failed.
    // Create a test CompressionService and replace the compressor with a mock.
    config.algorithmName = LZ4Compression.ALGORITHM_NAME;
    config.minimalSourceDataSizeInBytes = 1;
    CompressionService testService = new CompressionService(config, metrics);
    LZ4Compression mockCompression = Mockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    Mockito.doThrow(new RuntimeException("Compress failed.")).when(mockCompression).compress(
        Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
    testService.getAllCompressions().put(LZ4Compression.ALGORITHM_NAME, mockCompression);
    BlobProperties blobProperties =
        new BlobProperties(1234L, "testServiceID", "2222", "text/123", true, 12345L, (short) 111, (short) 222, true,
            "testTag", null, "testFileName");
    ByteBuf compressedBuffer = testService.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    Assert.assertEquals(1, metrics.compressErrorRate.getCount());
    Assert.assertEquals(1, metrics.compressError_CompressFailed.getCount());
  }

  @Test
  public void compressLowRatio() throws CompressionException {
    CompressionConfig config = new CompressionConfig();
    byte[] sourceBuffer = "Test Message for testing purpose.  The Message is part of the testing message.".getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());

    config.minimalSourceDataSizeInBytes = 1;
    BlobProperties blobProperties =
        new BlobProperties(1234L, "testServiceID", "2222", "text/123", true, 12345L, (short) 111, (short) 222, true,
            "testTag", null, "testFileName");

    // Test: Compression skipped because compression ratio is too low.
    metrics = new CompressionMetrics(new MetricRegistry());
    LZ4Compression mockCompression = Mockito.mock(LZ4Compression.class);
    Mockito.when(mockCompression.compress(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(new Pair<>(sourceBuffer.length, sourceBuffer));
    CompressionService testService = new CompressionService(config, metrics);
    testService.getAllCompressions().put(LZ4Compression.ALGORITHM_NAME, mockCompression);
    ByteBuf compressedBuffer = testService.compress(blobProperties, sourceByteBuf, true);
    Assert.assertEquals(compressedBuffer, sourceByteBuf);
    Assert.assertEquals(1, metrics.compressSkipRate.getCount());
    Assert.assertEquals(1, metrics.compressSkip_RatioTooSmall.getCount());
  }

  @Test
  public void compressSuccess() {
    CompressionConfig config = new CompressionConfig();
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message." +
        "Test Message for testing purpose.  The Message is part of the testing message." +
        "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());

    // Test: Happy case - compression succeed partial full.
    config.minimalCompressRatio = 1.0;
    config.minimalSourceDataSizeInBytes = 1;
    BlobProperties blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "text/123",
        true, 12345L, (short) 111, (short) 222, true, "testTag",
        null, "testFileName");
    metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);
    ByteBuf compressedBuffer = service.compress(blobProperties, sourceByteBuf, false);
    Assert.assertNotEquals(sourceByteBuf.readableBytes(), compressedBuffer.readableBytes());
    Assert.assertEquals(1, metrics.compressAcceptRate.getCount());

    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics(ZstdCompression.ALGORITHM_NAME);
    Assert.assertTrue(algorithmMetrics.compressRatioPercent.getCount() > 0);
    Assert.assertEquals(1, algorithmMetrics.compressRate.getCount());
    Assert.assertTrue(algorithmMetrics.partialFullCompressTimeInMicroseconds.getCount() > 0);
    Assert.assertTrue(algorithmMetrics.partialFullCompressSpeedMBPerSec.getCount() > 0);

    // Happy case - compression succeed full chunk.
    compressedBuffer = service.compress(blobProperties, sourceByteBuf, true);
    Assert.assertNotEquals(sourceByteBuf.readableBytes(), compressedBuffer.readableBytes());
    Assert.assertEquals(2, metrics.compressAcceptRate.getCount());

    algorithmMetrics = metrics.getAlgorithmMetrics(ZstdCompression.ALGORITHM_NAME);
    Assert.assertTrue(algorithmMetrics.compressRatioPercent.getCount() > 0);
    Assert.assertEquals(2, algorithmMetrics.compressRate.getCount());
    Assert.assertTrue(algorithmMetrics.fullSizeCompressTimeInMicroseconds.getCount() > 0);
    Assert.assertTrue(algorithmMetrics.fullSizeCompressSpeedMBPerSec.getCount() > 0);
  }

  @Test
  public void decompress_ConversionFailed() throws CompressionException {
    // Test: Decompress failed due to byte conversion throws.
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();

    CompressionConfig config = new CompressionConfig();
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);
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
    Assert.assertEquals(1, metrics.decompressError_BufferConversion.getCount());
  }

  @Test
  public void decompress_InvalidBuffer() {
    // Test: Decompress failed due to byte conversion throws.
    CompressionConfig config = new CompressionConfig();
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);

    // Test: buffer does not contain algorithm name. (name size is 100);
    final byte[] badCompressedBuffer = new byte[]{1, 100, 1, 2, 3, 4, 5};
    Exception ex = TestUtils.getException(
        () -> service.decompress(Unpooled.wrappedBuffer(badCompressedBuffer), badCompressedBuffer.length));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(1, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressError_BufferTooSmall.getCount());

    // Test: algorithm name does not exist.
    final byte[] badCompressedBuffer2 = new byte[]{1, 4, 65, 66, 67, 68, 5, 1, 2, 3, 4, 5};
    ex = TestUtils.getException(
        () -> service.decompress(Unpooled.wrappedBuffer(badCompressedBuffer2), badCompressedBuffer2.length));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(2, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressError_UnknownAlgorithmName.getCount());
  }

  @Test
  public void decompress_algorithmThrows() throws CompressionException {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    Pair<Integer, byte[]> compressedInfo = new LZ4Compression().compress(sourceBuffer);
    ByteBuf compressedBuffer = Unpooled.wrappedBuffer(compressedInfo.getSecond());

    CompressionConfig config = new CompressionConfig();
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);

    // Test: decompress() throws exception.
    // Create a test CompressionService and replace the compressor with a mock.
    LZ4Compression mockCompression = Mockito.mock(LZ4Compression.class, Mockito.CALLS_REAL_METHODS);
    Mockito.doThrow(new RuntimeException("Decompress failed.")).when(mockCompression).decompress(
        Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
    service.getAllCompressions().add(mockCompression);
    Exception ex = TestUtils.getException(() -> service.decompress(compressedBuffer, compressedBuffer.readableBytes()));
    Assert.assertTrue(ex instanceof CompressionException);
    Assert.assertEquals(1, metrics.decompressErrorRate.getCount());
    Assert.assertEquals(1, metrics.decompressError_DecompressFailed.getCount());
  }

  @Test
  public void decompressSuccess() throws CompressionException {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    Pair<Integer, byte[]> compressedInfo = new LZ4Compression().compress(sourceBuffer);
    ByteBuf compressedBuffer = Unpooled.wrappedBuffer(compressedInfo.getSecond(), 0, compressedInfo.getFirst());

    // Decompressed full chunk.
    CompressionConfig config = new CompressionConfig();
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);
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
    Assert.assertTrue(algorithmMetrics.partialFullDecompressTimeInMicroseconds.getCount() > 0);
  }

  @Test
  public void convertByteBufToByteArray() {
    CompressionConfig config = new CompressionConfig();
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);

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
  public void compressThenDecompress() throws CompressionException {
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();

    // Decompressed full chunk.
    CompressionConfig config = new CompressionConfig();
    config.minimalSourceDataSizeInBytes = 1;
    config.minimalCompressRatio = 1.0;
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    CompressionService service = new CompressionService(config, metrics);

    // Compress the source buffer.
    BlobProperties blobProperties = new BlobProperties(1234L, "testServiceID", "2222", "text/123",
        true, 12345L, (short) 111, (short) 222, true, "testTag",
        null, "testFileName");
    ByteBuf compressedBuffer = service.compress(blobProperties, Unpooled.wrappedBuffer(sourceBuffer), false);
    Assert.assertNotEquals(sourceBuffer.length, compressedBuffer.readableBytes());
    Assert.assertEquals(1, metrics.compressAcceptRate.getCount());


    // Decompress the source buffer.
    ByteBuf decompressedBuffer = service.decompress(compressedBuffer, sourceBuffer.length);
    Assert.assertEquals(sourceBuffer.length, decompressedBuffer.readableBytes());
    Assert.assertEquals(1, metrics.decompressSuccessRate.getCount());
    Assert.assertTrue(metrics.decompressExpandSizeBytes.getCount() > 0);
    Assert.assertTrue(Arrays.equals(decompressedBuffer.array(), sourceBuffer));

    // Zstd is the default compression algorithm.
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics(ZstdCompression.ALGORITHM_NAME);
    Assert.assertEquals(1, algorithmMetrics.decompressRate.getCount());
    Assert.assertTrue(algorithmMetrics.fullSizeDecompressTimeInMicroseconds.getCount() > 0);
  }
}