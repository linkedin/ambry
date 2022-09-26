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

import com.github.ambry.compression.Compression;
import com.github.ambry.compression.CompressionException;
import com.github.ambry.compression.CompressionMap;
import com.github.ambry.compression.LZ4Compression;
import com.github.ambry.compression.ZstdCompression;
import com.github.ambry.config.CompressionConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression service is thread-safe singleton that apply compression and decompression.
 * Its implementation is backed by the CompressionMap in the common library.
 */
public class CompressionService {

  private static final Logger logger = LoggerFactory.getLogger(CompressionService.class);

  // MB/sec = (byte/microsecond * MB/1024*1024bytes * 1000000Microseconds/sec)
  //        = 1MB/1.048576sec
  private static final double BytePerMicrosecondToMBToSec = 1/1.048576;

  private final CompressionConfig compressionConfig;
  private final CompressionMetrics compressionMetrics;

  // When a new compression algorithm is introduced, add the new algorithm here.
  // WARNING: Do not remove compression algorithms from this list.  See {@code Compression} interface for detail.
  private final CompressionMap allCompressions = CompressionMap.of(new ZstdCompression(), new LZ4Compression());

  /**
   * Create an instance of the compression service using the compression config and metrics.
   * The default compressor is loaded from config, if available.  If not available, ZStd is the default.
   * @param config Compression config.
   * @param metrics Compression metrics.
   */
  public CompressionService(CompressionConfig config, CompressionMetrics metrics) {
    compressionConfig = config;
    compressionMetrics = metrics;
  }

  /**
   * Get the compression map that contains all compression algorithms.
   * @return The compression map.
   */
  public CompressionMap getAllCompressions() {
    return allCompressions;
  }

  /**
   * Get the default compressor instance from the compressor name specified in config.
   * If the name is missing in config or not found in compression map, return null.
   * @param algorithmName The compression algorithm name.
   * @return The compression instance if found; null if not found.
   */
  private Compression getDefaultCompressor(String algorithmName) {
    // Config has selected a compression algorithm, set it.
    Compression defaultCompressor = allCompressions.get(algorithmName);
    if (defaultCompressor == null) {
      // Emit metrics for default compressor name does not exist.
       compressionMetrics.compressError_ConfigInvalidCompressorName.inc();
      logger.error("The default compressor algorithm, " + algorithmName
          + ", specified in config does not exist.  This config is ignored.");
      return allCompressions.get(ZstdCompression.ALGORITHM_NAME);
    }

    return defaultCompressor;
  }

  /**
   * Apply compression if compression is enabled and content-type is compressible.
   * If the compressed data size is smaller than original data by certain % (exceeds threshold), the compressed data
   * is accepted and return compressed data; otherwise, compression is discarded and return original data.
   *
   * @param chunkBuffer The PutChunk buffer to compress.
   * @return Returns a new compressed buffer.  It returns chunkBuffer is no compression is applied.
   */
  public ByteBuf compress(BlobProperties blobProperties, ByteBuf chunkBuffer, boolean isFullChunk) {
    Objects.requireNonNull(blobProperties, "blobProperties cannot be null.");
    Objects.requireNonNull(chunkBuffer, "blobProperties cannot be null.");

    // Return false if compression is disabled.  No need to emit metrics.
    if (!compressionConfig.isCompressionEnabled) {
      logger.info("No compression applied because compression is disabled.");
      return chunkBuffer;
    }

    // Check content-encoding. BlobProperty.ContentEncoding contains a string, do not apply compression.
    // Note: Calling getBlobProperties() throws if blob properties is null, so it never returns null.
    String contentEncoding = blobProperties.getContentEncoding();
    if (compressionConfig.isSkipWithContentEncoding && !Utils.isNullOrEmpty(contentEncoding)) {
      logger.info("No compression applied because compression content is encoded with " + contentEncoding);
      // Emit metrics to count how many compressions skipped due to encoded data.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkip_ContentEncoding.inc();
      return chunkBuffer;
    }

    // Check the content-type.
    String contentType = blobProperties.getContentType();
    if (!compressionConfig.isCompressibleContentType(contentType)) {
      logger.info("No compression applied because content-type " + contentType + " is incompressible.");
      // Emit metrics to count how many compressions skipped due to incompressible chunks based on content-type.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkip_ContentTypeFiltering.inc();
      return chunkBuffer;
    }

    // Check the blob size.  If it's too small, do not compress.
    int sourceDataSize = chunkBuffer.readableBytes();
    if (sourceDataSize < compressionConfig.minimalSourceDataSizeInBytes) {
      logger.info("No compression applied because source data size " + sourceDataSize + " is smaller than minimal size "
          + compressionConfig.minimalSourceDataSizeInBytes);
      // Emit metrics to count how many compressions skipped due to small size.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkip_SizeTooSmall.inc();
      return chunkBuffer;
    }

    // Convert ByteBuf to byte[] before applying compression.
    byte[] sourceBuffer;
    int sourceBufferOffset;
    try {
      Triple<byte[], Integer, Integer> arrayBuffer = convertByteBufToByteArray(chunkBuffer);
      sourceBuffer = arrayBuffer.getLeft();
      sourceBufferOffset = arrayBuffer.getMiddle();
    }
    catch (Exception ex) {
      logger.error(String.format("Cannot compress source data because failed to convert ByteBuf to byte[]."
          + "chunkBuffer.hasArray() = %s, chunkBuffer.readableBytes = %d, chunkBuffer.arrayOffset = %d",
          chunkBuffer.hasArray(), chunkBuffer.readableBytes(), chunkBuffer.arrayOffset()), ex);

      // Emit metrics for count how many compressions skipped due to buffer conversion error.
      compressionMetrics.compressErrorRate.mark();
      compressionMetrics.compressError_BufferConversion.inc();
      return chunkBuffer;
    }

    // Use default compressor to compress the data.
    Pair<Integer, byte[]> compressResult;
    Compression defaultCompressor = getDefaultCompressor(compressionConfig.algorithmName);
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = compressionMetrics.getAlgorithmMetrics(defaultCompressor.getAlgorithmName());
    try {
      algorithmMetrics.compressRate.mark();
      long startTime = System.nanoTime();
      compressResult = defaultCompressor.compress(sourceBuffer, sourceBufferOffset, sourceDataSize);
      long durationMicroseconds = (System.nanoTime() - startTime)/1000;

      // Compress succeeded, emit metrics.
      // MB/sec = (#bytes/#microseconds) x BytesPerMicroSecondsToMBToSec
      long speedInMBPerSec = (long) (BytePerMicrosecondToMBToSec * sourceDataSize / (double) durationMicroseconds);
      if (isFullChunk) {
        algorithmMetrics.fullSizeCompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.fullSizeCompressSpeedMBPerSec.update(speedInMBPerSec);
      } else {
        algorithmMetrics.partialFullCompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.partialFullCompressSpeedMBPerSec.update(speedInMBPerSec);
      }
      algorithmMetrics.compressRatioPercent.update((long) (100.0 * sourceDataSize / compressResult.getFirst()));
    }
    catch (Exception ex) {
      logger.error(String.format("Compress failed.  sourceBuffer.Length = %d, offset = %d, size = %d.",
          sourceBuffer.length, sourceBufferOffset, sourceDataSize), ex);

      // Emit metrics to count how many compressions skipped due to compression internal failure.
      algorithmMetrics.compressError.inc();
      compressionMetrics.compressErrorRate.mark();
      compressionMetrics.compressError_CompressFailed.inc();
      return chunkBuffer;
    }

    // Check whether the compression ratio is greater than threshold.
    int compressedDataSize = compressResult.getFirst();
    double compressionRatio = sourceDataSize / (double) compressedDataSize;
    if (compressionRatio < compressionConfig.minimalCompressRatio) {
      logger.info("Compression discarded because compression ratio " + compressionRatio
          + " is smaller than config's minimal ratio " + compressionConfig.minimalCompressRatio
          + ".  Source content type is " + contentType + ", source data size is " + sourceDataSize
          + " and compressed data size is " + compressedDataSize);

      // Emit metrics to count how many compressions skipped due to compression ratio too small.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkip_RatioTooSmall.inc();
      return chunkBuffer;
    }

    // Compression is accepted, emit metrics.
    compressionMetrics.compressAcceptRate.mark();
    compressionMetrics.compressReduceSizeBytes.inc(sourceDataSize - compressedDataSize);
    return Unpooled.wrappedBuffer(ByteBuffer.wrap(compressResult.getSecond(), 0, compressedDataSize));
  }

  /**
   * Convert ByteBuf to byte array with offset and size.
   * It does not advance the byteBuf position.
   *
   * @param byteBuf The buffer to read from.
   * @return The triple of buffer, buffer offset, and data size.
   */
   Triple<byte[], Integer, Integer> convertByteBufToByteArray(ByteBuf byteBuf) {

    // Convert chunk's ByteBuf to byte[] before applying compression.
    byte[] sourceBuffer;
    int sourceBufferOffset;
    int sourceDataSize = byteBuf.readableBytes();

    // If ByteBuf is backed by an array.  Just access array directly.
    if (byteBuf.hasArray()) {
      sourceBuffer = byteBuf.array();
      sourceBufferOffset = byteBuf.arrayOffset();
      return Triple.of(sourceBuffer, sourceBufferOffset, sourceDataSize);
    }

    // If ByteBuf contains only 1 NIO buffer, unwrap the buffer.
    if (byteBuf.nioBufferCount() == 1) {
      ByteBuffer nioBuffer = byteBuf.nioBuffers()[0];
      if (nioBuffer.hasArray()) {
        sourceBuffer = nioBuffer.array();
        sourceBufferOffset = nioBuffer.arrayOffset();
        return Triple.of(sourceBuffer, sourceBufferOffset, sourceDataSize);
      }
    }

    // Mark the chunk buffer.  We will restore the readerIndex if buffer is incompressible.
    // ByteBuf is backed by other format. Regardless, copy the data into an array.
    byteBuf.markReaderIndex();
    try {
      sourceBufferOffset = 0;
      sourceBuffer = new byte[sourceDataSize];
      byteBuf.readBytes(sourceBuffer);
    }
    finally {
      // Assume no compression, revert the reader index (that advanced due to reading bytes).
      byteBuf.resetReaderIndex();
    }

    return Triple.of(sourceBuffer, sourceBufferOffset, sourceDataSize);
  }

  /**
   * Decompress the specified compressed buffer.
   *
   * @param compressedBuffer The compressed buffer.
   * @return The decompressor used and the decompressed buffer.
   * @throws CompressionException This exception is thrown when internal compression error or cannot find decompressor.
   */
  public ByteBuf decompress(ByteBuf compressedBuffer, int fullChunkSize) throws CompressionException {
    Objects.requireNonNull(compressedBuffer, "compressedBuffer");

    // buffer, offset, and size.
    Triple<byte[], Integer, Integer> bufferInfo;
    try {
      bufferInfo = convertByteBufToByteArray(compressedBuffer);
    }
    catch (Exception ex) {
      logger.error("Cannot decompress buffer.  Unable to convert ByteBuf to byte[].", ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressError_BufferConversion.inc();
      throw new CompressionException("Decompressed failed because unable to convert buffer to byte[].");
    }

    byte[] buffer = bufferInfo.getLeft();
    int bufferOffset = bufferInfo.getMiddle();
    int dataSize = bufferInfo.getRight();
    String algorithmName;
    try {
      algorithmName = allCompressions.getAlgorithmName(buffer, bufferOffset, dataSize);
    }
    catch (Exception ex) {
      logger.error("Cannot decompress buffer.  Unable to convert get compression algorithm name in buffer.", ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressError_BufferTooSmall.inc();
      throw new CompressionException("Decompressed failed because compressed buffer too small.");
    }

    Compression decompressor = allCompressions.getByName(algorithmName);
    if (decompressor == null) {
      logger.error("Cannot decompress buffer.  Cannot find decompressor for algorithm name " + algorithmName);
      // Emit metrics to count how many unknown name/missing algorithm registration.
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressError_UnknownAlgorithmName.inc();
      throw new CompressionException("Decompression failed due to unknown algorithm name " + algorithmName);
    }

    // Apply decompression, then wrap result in ByteBuf.
    byte[] decompressedBuffer;
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = compressionMetrics.getAlgorithmMetrics(algorithmName);
    try {
      algorithmMetrics.decompressRate.mark();
      long startTime = System.nanoTime();
      decompressedBuffer = decompressor.decompress(buffer, bufferOffset, dataSize);
      long durationMicroseconds = (System.nanoTime() - startTime)/1000;

      long speedInMBPerSec = (long) (BytePerMicrosecondToMBToSec * decompressedBuffer.length / (double) durationMicroseconds);
      if (decompressedBuffer.length == fullChunkSize) {
        algorithmMetrics.fullSizeDecompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.fullSizeDecompressSpeedMBPerSec.update(speedInMBPerSec);
      } else {
        algorithmMetrics.partialFullDecompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.partialFullDecompressSpeedMBPerSec.update(speedInMBPerSec);
      }
    }
    catch (Exception ex) {
      logger.error("Decompression failed.  Algorithm name " + algorithmName + ", compressed size = " + dataSize, ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressError_DecompressFailed.inc();
      algorithmMetrics.decompressError.inc();

      // Throw exception.
      if (!(ex instanceof CompressionException)) {
        ex = new CompressionException("Decompress failed with an exception.  Algorithm name " + algorithmName +
            ", compressed size = " + dataSize, ex);
      }
      throw (CompressionException) ex;
    }

    // Decompress succeeded, emit metrics.
    compressionMetrics.decompressSuccessRate.mark();
    compressionMetrics.decompressExpandSizeBytes.inc(decompressedBuffer.length - dataSize);
    return Unpooled.wrappedBuffer(ByteBuffer.wrap(decompressedBuffer));
  }
}
