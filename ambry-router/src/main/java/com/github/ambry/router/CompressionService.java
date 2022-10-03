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

  // MB/sec = (byte/microsecond * MB/1024*1024bytes * 1000000Microseconds/sec) = 1 MB/1.048576 sec
  private static final double BytePerMicrosecondToMBPerSec = 1/1.048576;

  private final CompressionConfig compressionConfig;
  private final CompressionMetrics compressionMetrics;
  private Compression defaultCompressor;

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

    // Check whether the default compressor name is valid in the config.  If not, fall back to Zstd.
    Compression compressor = allCompressions.get(compressionConfig.algorithmName);
    if (compressor == null) {
      compressor = allCompressions.get(ZstdCompression.ALGORITHM_NAME);

      // Emit metrics for default compressor name in config does not exist.
      compressionMetrics.compressErrorConfigInvalidCompressorName.inc();
      logger.error("The default compressor algorithm, " + compressionConfig.algorithmName
          + ", specified in config does not exist.  This default has changed to " + ZstdCompression.ALGORITHM_NAME);
    }
    defaultCompressor = compressor;
  }

  /**
   * Get the compression map that contains all compression algorithms.
   * @return The compression map.
   */
  public CompressionMap getAllCompressions() {
    return allCompressions;
  }

  /**
   * Get the default compressor.
   * @return The default compressor.
   */
  public Compression getDefaultCompressor() {
    return defaultCompressor;
  }

  /**
   * Set the default compressor to use.
   * @param newDefaultCompressor The new default compressor.
   */
  public void setDefaultCompressor(Compression newDefaultCompressor) {
    Objects.requireNonNull(newDefaultCompressor, "newDefaultCompressor");
    defaultCompressor = newDefaultCompressor;
  }

  /**
   * Check whether the blob is compressible based on the blob properties.
   *
   * @param blobProperties The blob property that is common to all chunks. It helps to detect whether blob is
   *                       compressible or not.
   * @return True if blob is compressible based on the blob properties; FALSE if blob is not compressible.
   */
  public boolean isBlobCompressible(BlobProperties blobProperties) {
    Objects.requireNonNull(blobProperties, "blobProperties cannot be null.");

    // Return false if compression is disabled.  No need to emit metrics.
    if (!compressionConfig.isCompressionEnabled) {
      return false;
    }

    // Check content-encoding. If BlobProperty.ContentEncoding contains a string, do not apply compression.
    String contentEncoding = blobProperties.getContentEncoding();
    if (compressionConfig.isSkipWithContentEncoding && !Utils.isNullOrEmpty(contentEncoding)) {
      logger.trace("No compression applied because blob is encoded with " + contentEncoding);
      // Emit metrics to count how many compressions skipped due to encoded data.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipContentEncoding.inc();
      return false;
    }

    // Check the content-type.  If content type is incompressible based on config, skip compression.
    String contentType = blobProperties.getContentType();
    if (!compressionConfig.isCompressibleContentType(contentType)) {
      logger.trace("No compression applied because content-type " + contentType + " is incompressible.");
      // Emit metrics to count how many compressions skipped due to incompressible chunks based on content-type.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipContentTypeFiltering.inc();
      return false;
    }

    // For all other case, compression should be enabled.
    return true;
  }

  /**
   * Apply compression to a chunk buffer if compression is enabled and content-type is compressible.
   * It applies compression config to determine whether to accept or reject the compressed result.
   * For example, if the compressed data size is smaller than original data by certain % (exceeds threshold),
   * the compressed data is accepted and return compressed data; otherwise, compression is discarded.
   * This method emits metrics along the process.  It returns null if failed instead of throwing exceptions.
   *
   * @param chunkBuffer The PutChunk buffer to compress.
   * @param isFullChunk Whether this is a full size chunk (4MB) or smaller.
   * @return Returns a new compressed buffer.  It returns null is no compression is applied.
   */
  public ByteBuf compressChunk(ByteBuf chunkBuffer, boolean isFullChunk) {
    Objects.requireNonNull(chunkBuffer, "blobProperties cannot be null.");

    // Check the blob size.  If it's too small, do not compress.
    int sourceDataSize = chunkBuffer.readableBytes();
    if (sourceDataSize < compressionConfig.minimalSourceDataSizeInBytes) {
      logger.trace("No compression applied because source data size " + sourceDataSize + " is smaller than minimal size "
          + compressionConfig.minimalSourceDataSizeInBytes);
      // Emit metrics to count how many compressions skipped due to small size.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipSizeTooSmall.inc();
      return null;
    }

    // Convert ByteBuf to byte[] before applying compression.
    byte[] sourceBuffer;
    int sourceBufferOffset;
    try {
      Triple<byte[], Integer, Integer> bufferInfo = convertByteBufToByteArray(chunkBuffer);
      sourceBuffer = bufferInfo.getLeft();
      sourceBufferOffset = bufferInfo.getMiddle();
      sourceDataSize = bufferInfo.getRight();
    } catch (Exception ex) {
      logger.error(String.format("Cannot compress source data because failed to convert ByteBuf to byte[]."
              + "chunkBuffer.hasArray() = %s, chunkBuffer.readableBytes = %d, chunkBuffer.arrayOffset = %d",
          chunkBuffer.hasArray(), chunkBuffer.readableBytes(), chunkBuffer.arrayOffset()), ex);

      // Emit metrics for count how many compressions skipped due to buffer conversion error.
      compressionMetrics.compressErrorRate.mark();
      compressionMetrics.compressErrorBufferConversion.inc();
      return null;
    }

    // Apply compression.
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = compressionMetrics.getAlgorithmMetrics(defaultCompressor.getAlgorithmName());
    Pair<Integer, byte[]> compressResult;
    try {
      algorithmMetrics.compressRate.mark();
      long startTime = System.nanoTime();
      compressResult = defaultCompressor.compress(sourceBuffer, sourceBufferOffset, sourceDataSize);
      long durationMicroseconds = (System.nanoTime() - startTime)/1000;

      // Compress succeeded, emit metrics. MB/sec = (#bytes/#microseconds) x BytesPerMicroSecondsToMBToSec
      long speedInMBPerSec = (long) (BytePerMicrosecondToMBPerSec * sourceDataSize / (double) durationMicroseconds);
      if (isFullChunk) {
        algorithmMetrics.fullSizeCompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.fullSizeCompressSpeedMBPerSec.update(speedInMBPerSec);
      } else {
        algorithmMetrics.smallSizeCompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.smallSizeCompressSpeedMBPerSec.update(speedInMBPerSec);
      }
      algorithmMetrics.compressRatioPercent.update((long) (100.0 * sourceDataSize / compressResult.getFirst()));
    } catch (Exception ex) {
      logger.error(String.format("Compress failed.  sourceBuffer.Length = %d, offset = %d, size = %d.",
          sourceBuffer.length, sourceBufferOffset, sourceDataSize), ex);

      // Emit metrics to count how many compressions skipped due to compression internal failure.
      algorithmMetrics.compressError.inc();
      compressionMetrics.compressErrorRate.mark();
      compressionMetrics.compressErrorCompressFailed.inc();
      return null;
    }

    // Check whether the compression ratio is greater than threshold.
    int compressedDataSize = compressResult.getFirst();
    double compressionRatio = sourceDataSize / (double) compressedDataSize;
    if (compressionRatio < compressionConfig.minimalCompressRatio) {
      logger.trace("Compression discarded because compression ratio " + compressionRatio
          + " is smaller than config's minimal ratio " + compressionConfig.minimalCompressRatio
          + ".  Source content size is " + sourceDataSize + " and compressed data size is " + compressedDataSize);

      // Emit metrics to count how many compressions skipped due to compression ratio too small.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipRatioTooSmall.inc();
      return null;
    }

    // Compression is accepted, emit metrics.
    compressionMetrics.compressAcceptRate.mark();
    compressionMetrics.compressReduceSizeBytes.inc(sourceDataSize - compressedDataSize);
    return Unpooled.wrappedBuffer(ByteBuffer.wrap(compressResult.getSecond(), 0, compressedDataSize));
  }

  /**
   * Decompress the specified compressed buffer.
   *
   * @param compressedBuffer The compressed buffer.
   * @param fullChunkSize The size of a full chunk.  It is used to select metrics to emit.
   * @return The decompressor used and the decompressed buffer.
   * @throws CompressionException This exception is thrown when internal compression error or cannot find decompressor.
   */
  public ByteBuf decompress(ByteBuf compressedBuffer, int fullChunkSize) throws CompressionException {
    Objects.requireNonNull(compressedBuffer, "compressedBuffer");

    // buffer, offset, and size.
    Triple<byte[], Integer, Integer> bufferInfo;
    try {
      bufferInfo = convertByteBufToByteArray(compressedBuffer);
    } catch (Exception ex) {
      logger.error("Cannot decompress buffer.  Unable to convert ByteBuf to byte[].", ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorBufferConversion.inc();
      throw new CompressionException("Decompressed failed because unable to convert buffer to byte[].");
    }

    byte[] buffer = bufferInfo.getLeft();
    int bufferOffset = bufferInfo.getMiddle();
    int dataSize = bufferInfo.getRight();
    String algorithmName;
    try {
      algorithmName = allCompressions.getAlgorithmName(buffer, bufferOffset, dataSize);
    } catch (Exception ex) {
      logger.error("Cannot decompress buffer.  Unable to convert get compression algorithm name in buffer.", ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorBufferTooSmall.inc();
      throw new CompressionException("Decompressed failed because compressed buffer too small.");
    }

    Compression decompressor = allCompressions.getByName(algorithmName);
    if (decompressor == null) {
      logger.error("Cannot decompress buffer.  Cannot find decompressor for algorithm name " + algorithmName);
      // Emit metrics to count how many unknown name/missing algorithm registration.
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorUnknownAlgorithmName.inc();
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

      long speedInMBPerSec = durationMicroseconds == 0 ? 0 :
          (long) (BytePerMicrosecondToMBPerSec * decompressedBuffer.length / (double) durationMicroseconds);
      if (decompressedBuffer.length == fullChunkSize) {
        algorithmMetrics.fullSizeDecompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.fullSizeDecompressSpeedMBPerSec.update(speedInMBPerSec);
      } else {
        algorithmMetrics.smallSizeDecompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.smallSizeDecompressSpeedMBPerSec.update(speedInMBPerSec);
      }
    } catch (Exception ex) {
      logger.error("Decompression failed.  Algorithm name " + algorithmName + ", compressed size = " + dataSize, ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorDecompressFailed.inc();
      algorithmMetrics.decompressError.inc();

      // Throw exception.
      Exception rethrowException = ex;
      if (!(ex instanceof CompressionException)) {
        rethrowException = new CompressionException("Decompress failed with an exception.  Algorithm name " + algorithmName +
            ", compressed size = " + dataSize, ex);
      }
      throw (CompressionException) rethrowException;
    }

    // Decompress succeeded, emit metrics.
    compressionMetrics.decompressSuccessRate.mark();
    compressionMetrics.decompressExpandSizeBytes.inc(decompressedBuffer.length - dataSize);
    return Unpooled.wrappedBuffer(ByteBuffer.wrap(decompressedBuffer));
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
}
