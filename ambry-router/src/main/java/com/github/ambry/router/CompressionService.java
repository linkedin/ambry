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
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.tuple.Triple;
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
    Objects.requireNonNull(metrics, "metrics");
    compressionMetrics = metrics;
    if (config == null) config = new CompressionConfig();

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
   * @param chunkBuffer The PutChunk buffer to compress.
   * @param isFullChunk Whether this is a full size chunk (4MB) or smaller.
   * @return Returns a new compressed buffer.  It returns null is no compression is applied.
   */
  public ByteBuf compressChunk(ByteBuf chunkBuffer, boolean isFullChunk) {
    Objects.requireNonNull(chunkBuffer, "blobProperties cannot be null.");

    // Check the blob size.  If it's too small, do not compress.
    int sourceDataSize = chunkBuffer.readableBytes();
    if (sourceDataSize < minimalSourceDataSizeInBytes) {
      logger.trace("No compression applied because source data size " + sourceDataSize + " is smaller than minimal size "
          + minimalSourceDataSizeInBytes);
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
    if (compressionRatio < minimalCompressRatio) {
      logger.trace("Compression discarded because compression ratio " + compressionRatio
          + " is smaller than config's minimal ratio " + minimalCompressRatio
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
