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
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression service is thread-safe singleton that apply compression and decompression.
 * Its implementation is backed by the CompressionMap in the common library.
 * The compression service class is self-contained that contains all properties for controlling compression/decompression.
 * The properties are initialized using a specific or default compression config.
 */
public class CompressionService {

  // MB/sec = (byte/microsecond * MB/1024*1024bytes * 1000000Microseconds/sec) = 1 MB/1.048576 sec
  private static final double BytePerMicrosecondToMBPerSec = 1/1.048576;
  private static final Logger logger = LoggerFactory.getLogger(CompressionService.class);
  private static final PooledByteBufAllocator byteBufAllocator = PooledByteBufAllocator.DEFAULT;

  // Emit all compression metrics here.
  private final CompressionMetrics compressionMetrics;

  // When a new compression algorithm is introduced, add the new algorithm here.
  // WARNING: Do not remove compression algorithms from this list.  See {@code Compression} interface for detail.
  public final CompressionMap allCompressions = CompressionMap.of(new ZstdCompression(), new LZ4Compression());

  // Properties that controls the behavior of this service.
  // For now, those properties are public fields.  With Lombok support, they should be private with Getter and Setter.

  /**
   * The default compressor use by compress().
   */
  public Compression defaultCompressor;

  /**
   * Whether compression is enabled.
   */
  public boolean isCompressionEnabled;

  /**
   * Whether compression is skipped when the content-encoding is present in the request.
   */
  public boolean isSkipWithContentEncoding;

  /**
   * The minimal source/chunk size in bytes in order to qualify for compression.
   * Chunk size smaller than this size will not be compressed.
   */
  public int minimalSourceDataSizeInBytes;

  /**
   * The minimal compression ratio in order to keep compressed content.
   * If the size of compressed content is almost the same size as original content, discard the compressed content.
   * Compression ratio is defined as OriginalSize/CompressedSize.  Normally, higher is better.
   */
  public double minimalCompressRatio;

  /**
   * The set contains a list of compressible content-types and content-prefixes.
   * The key (content type) must be lower-case only.
   * This set is generated from the compressibleContentTypesCSV property.
   */
  public final Set<String> compressibleContentTypes;

    /**
     * Create an instance of the compression service using the compression config and metrics.
     * Initialize the compressor properties using config.  If the default compressor in config is invalid or missing,
     * it falls back to ZStdCompression.
     * @param config Compression config.  If null, the default compression config is used.
     * @param metrics Compression metrics.  Cannot be null.
     */
  public CompressionService(CompressionConfig config, CompressionMetrics metrics) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(metrics, "metrics");
    compressionMetrics = metrics;

    // Initialize the properties using the config.
    isCompressionEnabled = config.isCompressionEnabled;
    isSkipWithContentEncoding = config.isSkipWithContentEncoding;;
    minimalSourceDataSizeInBytes = config.minimalSourceDataSizeInBytes;
    minimalCompressRatio = config.minimalCompressRatio;
    compressibleContentTypes = new HashSet<>();

    // Split the content-type list by comma, then convert them all to lower-case.
    Utils.splitString(config.compressibleContentTypesCSV, ",", item -> item.trim().toLowerCase(Locale.US),
        compressibleContentTypes);

    // Set the default compressor based on the config's selected algorithm name.  If nam eis invalid, fall back to Zstd.
    Compression compressor = allCompressions.get(config.algorithmName);
    if (compressor == null) {
      compressor = allCompressions.get(ZstdCompression.ALGORITHM_NAME);

      // Emit metrics for default compressor name in config does not exist.
      compressionMetrics.compressErrorConfigInvalidCompressorName.inc();
      logger.error("The default compressor algorithm, " + config.algorithmName
          + ", specified in config does not exist.  This default has changed to " + compressor.getAlgorithmName());
    }
    defaultCompressor = compressor;

    // TODO - Temporary deployment control code that will be removed after testing and deployment.
    if (config.isCompressionEnabled) {
      PutRequest.currentVersion = PutRequest.PUT_REQUEST_VERSION_V5;
    }
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
    if (!isCompressionEnabled) {
      return false;
    }

    // Check content-encoding. If BlobProperty.ContentEncoding contains a string, do not apply compression.
    String contentEncoding = blobProperties.getContentEncoding();
    if (isSkipWithContentEncoding && !Utils.isNullOrEmpty(contentEncoding)) {
      logger.trace("No compression applied because blob is encoded with " + contentEncoding);
      // Emit metrics to count how many compressions skipped due to encoded data.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipContentEncoding.inc();
      return false;
    }

    // Check the content-type.  If content type is incompressible based on config, skip compression.
    String contentType = blobProperties.getContentType();
    if (!isCompressibleContentType(contentType)) {
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
   * Check whether compression should be applied to the specified content type.
   * Support format of content-type (3 variations):
   * See <a href="https://www.geeksforgeeks.org/http-headers-content-type/">Content-Type spec</a> for detail.
   *   - text/html
   *   - text/html; charset=UTF-8
   *   - multipart/form-data; boundary=something
   * <p>
   * Content type format is "prefix/specific; option=value".
   * Content check is applied in this order.  If not in the list, it's not compressible.
   * 1. Check whether the full contentType parameter is specified in content-type set.  Example "text/html"
   * 2. If contentType parameter contains option (separated by ";"), remove the option and check again.
   *    For example, parameter content-type "text/html; charset=UTF-8", check config by content-type "text/html".
   * 3. Get the content-type prefix (the string before the "/" separator) and lookup prefix again.
   *    For example, parameter content-type "text/html; charset=UTF-8", check config by content-prefix "text".
   * @param contentType The HTTP content type.
   * @return TRUE if compression allowed; FALSE if compression not allowed.
   */
  boolean isCompressibleContentType(String contentType) {
    // If there is no content-type, assume it's other content-type.
    if (Utils.isNullOrEmpty(contentType)) {
      return false;
    }

    // If an exact match by the full content-type is found, it's compressible.
    String contentTypeLower = contentType.trim().toLowerCase();
    if (compressibleContentTypes.contains(contentTypeLower)) {
      return true;
    }

    // Check whether the content-type contains options separated by ";"
    int mimeSeparatorIndex = contentTypeLower.indexOf(';');
    if (mimeSeparatorIndex > 0) {
      String contentTypeWithoutOption = contentTypeLower.substring(0, mimeSeparatorIndex);
      if (compressibleContentTypes.contains(contentTypeWithoutOption)) {
        return true;
      }
    }

    // Check whether there's a match by content-type prefix.
    // For example, if input content-type is "text/csv", check whether "text" is compressible.
    int prefixSeparatorIndex = contentTypeLower.indexOf('/');
    if (prefixSeparatorIndex > 0) {
      String prefix = contentTypeLower.substring(0, prefixSeparatorIndex);
      if (compressibleContentTypes.contains(prefix)) {
        return true;
      }
    }

    // Content-type is not found in the allowed list, assume it's not compressible.
    return false;
  }

  /**
   * Apply compression to a chunk buffer if compression is enabled and content-type is compressible.
   * It applies compression config to determine whether to accept or reject the compressed result.
   * For example, if the compressed data size is smaller than original data by certain % (exceeds threshold),
   * the compressed data is accepted and return compressed data; otherwise, compression is discarded.
   * This method emits metrics along the process.  It returns null if failed instead of throwing exceptions.
   *
   * @param chunkByteBuf The PutChunk buffer to compress.  The chunkBuffer index will not be updated.
   * @param isFullChunk Whether this is a full size chunk (4MB) or smaller.
   * @param outputDirectBuffer Whether to output in direct buffer (True) or heap buffer (False).
   * @return Returns a new compressed buffer.  It returns null is no compression is applied.
   */
  public ByteBuf compressChunk(ByteBuf chunkByteBuf, boolean isFullChunk, boolean outputDirectBuffer) {
    Objects.requireNonNull(chunkByteBuf, "chunkByteBuf cannot be null.");

    // Check the blob size.  If it's too small, do not compress.
    int sourceDataSize = chunkByteBuf.readableBytes();
    if (sourceDataSize < minimalSourceDataSizeInBytes) {
      logger.trace("No compression applied because source data size " + sourceDataSize + " is smaller than minimal size "
          + minimalSourceDataSizeInBytes);
      // Emit metrics to count how many compressions skipped due to small size.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipSizeTooSmall.inc();
      return null;
    }

    // Apply compression.
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = compressionMetrics.getAlgorithmMetrics(defaultCompressor.getAlgorithmName());
    ByteBuf newChunkByteBuf = combineBuffer(chunkByteBuf, outputDirectBuffer, defaultCompressor.requireMatchingBufferType());
    ByteBuf compressedByteBuf = null;
    int actualCompressedByteBufferSize;
    try {
      algorithmMetrics.compressRate.mark();

      // Allocate compression buffer.
      ByteBuffer sourceByteBuffer = newChunkByteBuf.nioBuffer();
      int compressedByteBufSize = defaultCompressor.getCompressBufferSize(sourceByteBuffer.remaining());
      compressedByteBuf = outputDirectBuffer ? byteBufAllocator.directBuffer(compressedByteBufSize) :
          byteBufAllocator.heapBuffer(compressedByteBufSize);

      // Compressed buffer allocated, run compression.
      long startTime = System.nanoTime();
      ByteBuffer compressedByteBuffer = compressedByteBuf.nioBuffer(compressedByteBuf.readerIndex(),
          compressedByteBufSize);
      actualCompressedByteBufferSize = defaultCompressor.compress(sourceByteBuffer, compressedByteBuffer);
      compressedByteBuf.writerIndex(compressedByteBuf.writerIndex() + actualCompressedByteBufferSize);
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
      algorithmMetrics.compressRatioPercent.update((long) (100.0 * sourceDataSize / compressedByteBufSize));
    } catch (Exception ex) {
      logger.error(String.format("Compress failed.  sourceBuffer.capacity = %d, offset = %d, size = %d.",
          newChunkByteBuf.capacity(), newChunkByteBuf.readerIndex(), sourceDataSize), ex);

      // Emit metrics to count how many compressions skipped due to compression internal failure.
      algorithmMetrics.compressError.inc();
      compressionMetrics.compressErrorRate.mark();
      compressionMetrics.compressErrorCompressFailed.inc();
      if (compressedByteBuf != null) {
        compressedByteBuf.release();
      }
      return null;
    } finally {
      newChunkByteBuf.release();
    }

    // Check whether the compression ratio is greater than threshold.
    double compressionRatio = sourceDataSize / (double) actualCompressedByteBufferSize;
    if (compressionRatio < minimalCompressRatio) {
      logger.trace("Compression discarded because compression ratio " + compressionRatio
          + " is smaller than config's minimal ratio " + minimalCompressRatio
          + ".  Source content size is " + sourceDataSize + " and compressed data size is " + actualCompressedByteBufferSize);

      // Emit metrics to count how many compressions skipped due to compression ratio too small.
      compressionMetrics.compressSkipRate.mark();
      compressionMetrics.compressSkipRatioTooSmall.inc();
      compressedByteBuf.release();
      return null;
    }

    // Compression is accepted, emit metrics.
    compressionMetrics.compressAcceptRate.mark();
    compressionMetrics.compressReduceSizeBytes.inc(sourceDataSize - actualCompressedByteBufferSize);
    return compressedByteBuf;
  }

  /**
   * Decompress the specified compressed buffer.  compressedBuffer index will not be updated.
   *
   * @param compressedByteBuf The compressed buffer.
   * @param fullChunkSize The size of a full chunk.  It is used to select metrics to emit.
   * @param outputDirectBuffer Whether to output in direct buffer (True) or heap buffer (False).
   * @return The decompressor used and the decompressed buffer.
   * @throws CompressionException This exception is thrown when internal compression error or cannot find decompressor.
   */
  public ByteBuf decompress(ByteBuf compressedByteBuf, int fullChunkSize, boolean outputDirectBuffer) throws CompressionException {
    Objects.requireNonNull(compressedByteBuf, "compressedByteBuf");

    // Get name of compression algorithm.
    String algorithmName;
    try {
      algorithmName = allCompressions.getAlgorithmName(compressedByteBuf);
    } catch (Exception ex) {
      logger.error("Cannot decompress buffer.  Unable to convert get compression algorithm name in buffer.", ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorBufferTooSmall.inc();
      throw new CompressionException("Decompressed failed because compressed buffer too small.", ex);
    }

    // Get the compressor based on the compressor algorithm name.
    Compression decompressor = allCompressions.getByName(algorithmName);
    if (decompressor == null) {
      logger.error("Cannot decompress buffer.  Cannot find decompressor for algorithm name " + algorithmName);
      // Emit metrics to count how many unknown name/missing algorithm registration.
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorUnknownAlgorithmName.inc();
      throw new CompressionException("Decompression failed due to unknown algorithm name " + algorithmName);
    }

    // Apply decompression.
    CompressionMetrics.AlgorithmMetrics algorithmMetrics = compressionMetrics.getAlgorithmMetrics(algorithmName);
      algorithmMetrics.decompressRate.mark();

    ByteBuf decompressedByteBuf = null;
    int decompressedByteBufSize;
    ByteBuf newCompressedByteBuf = combineBuffer(compressedByteBuf, outputDirectBuffer, decompressor.requireMatchingBufferType());
    try {
      // Allocate decompression buffer.
      ByteBuffer compressedByteBuffer = newCompressedByteBuf.nioBuffer();
      decompressedByteBufSize = decompressor.getDecompressBufferSize(compressedByteBuffer);
      decompressedByteBuf = outputDirectBuffer ? byteBufAllocator.directBuffer(decompressedByteBufSize) :
          byteBufAllocator.heapBuffer(decompressedByteBufSize);

      // Buffer allocated, run decompression.
      ByteBuffer decompressedByteBuffer = decompressedByteBuf.nioBuffer(decompressedByteBuf.readerIndex(),
          decompressedByteBufSize);
      long startTime = System.nanoTime();
      decompressor.decompress(compressedByteBuffer, decompressedByteBuffer);
      decompressedByteBuf.writerIndex(decompressedByteBuf.writerIndex() + decompressedByteBufSize);
      long durationMicroseconds = (System.nanoTime() - startTime) / 1000;

      // Emit metrics related to this decompression.
      long speedInMBPerSec = durationMicroseconds == 0 ? 0
          : (long) (BytePerMicrosecondToMBPerSec * decompressedByteBufSize / (double) durationMicroseconds);
      if (decompressedByteBufSize == fullChunkSize) {
        algorithmMetrics.fullSizeDecompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.fullSizeDecompressSpeedMBPerSec.update(speedInMBPerSec);
      } else {
        algorithmMetrics.smallSizeDecompressTimeInMicroseconds.update(durationMicroseconds);
        algorithmMetrics.smallSizeDecompressSpeedMBPerSec.update(speedInMBPerSec);
      }
    } catch (Exception ex) {
      logger.error("Decompression failed.  Algorithm name " + algorithmName + ", compressed size = " +
          compressedByteBuf.readableBytes(), ex);
      compressionMetrics.decompressErrorRate.mark();
      compressionMetrics.decompressErrorDecompressFailed.inc();
      algorithmMetrics.decompressError.inc();

      // Throw exception.
      Exception rethrowException = ex;
      if (!(ex instanceof CompressionException)) {
        rethrowException = new CompressionException(
            "Decompress failed with an exception.  Algorithm name " + algorithmName + ", compressed size = "
                + compressedByteBuf.readableBytes(), ex);
      }
      if (decompressedByteBuf != null) {
        decompressedByteBuf.release();
      }
      throw (CompressionException) rethrowException;
    } finally {
      newCompressedByteBuf.release();
    }

    // Decompress succeeded, emit metrics.
    compressionMetrics.decompressSuccessRate.mark();
    compressionMetrics.decompressExpandSizeBytes.inc(decompressedByteBufSize - compressedByteBuf.readableBytes());
    return decompressedByteBuf;
  }

  /**
   * Convert a ByteBuf to a single buffer.  If source buffer is already single buffer, just return it.
   * If it is consisted of multiple buffers, combine them into 1 direct buffer.
   * Since this method may copy the source buffer, it generates a buffer that matches the compressor requirement based
   * on outputDirectBuffer and compressorRequireMatchingBuffer parameters.
   *
   * @param byteBuf The source buffer to read from.
   * @param outputDirectBuffer Whether the output buffer must be direct buffer.
   *                           True means output is direct buffer; False means output is Heap buffer.
   * @param compressorRequireMatchingBuffer Whether the source buffer and output buffer must match.
   * @return The new buffer.  The new buffer must be released when done reading.
   */
  private ByteBuf combineBuffer(ByteBuf byteBuf, boolean outputDirectBuffer, boolean compressorRequireMatchingBuffer) {
    // We need to copy the buffer if the source and target buffer mismatch, and they are required to match.
    boolean mustCopyBuffer = compressorRequireMatchingBuffer && (outputDirectBuffer != byteBuf.isDirect());

    // If the source buffer is already has 1 buffer, create an index duplicate (not the buffer).
    if (byteBuf.nioBufferCount() == 1 && !mustCopyBuffer) {
      return byteBuf.retainedDuplicate();
    }

    // Copy the buffer out without changing index.
    ByteBuf newBuffer = outputDirectBuffer ? byteBufAllocator.directBuffer(byteBuf.readableBytes()):
        byteBufAllocator.heapBuffer(byteBuf.readableBytes());
    byteBuf.getBytes(byteBuf.readerIndex(), newBuffer);
    return newBuffer;
  }
}
