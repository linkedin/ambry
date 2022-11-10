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

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;


/**
 * Zstandard compression algorithm using Zstandard native and JNI.
 * Zstandard has fast decompression speed and pretty good compression ratio.
 * It is not as fast as LZ4 but has higher compression ratio than LZ4.
 * More info about Zstandard in <a href="https://github.com/lz4/lz4">GitHub</a>.
 * <p>
 * Zstd compression level range is from negative 7 (fastest) to 22 (slowest in compression speed,
 * but best compression ratio), with level 3 as the default.
 */
public class ZstdCompression extends BaseCompressionWithLevel {

  /**
   * Name of this algorithm.
   */
  public static final String ALGORITHM_NAME = "ZSTD";

  /**
   * Get the unique name of this compression algorithm.
   * WARNING - Do not change the algorithm name.  See Compression interface for detail.
   * @return Name of this algorithm.
   */
  @Override
  public final String getAlgorithmName() {
    return ALGORITHM_NAME;
  }

  /**
   * Some compression algorithms like ZStd are picky on the source and destination buffer types.
   * In Zstd, either both buffers are Heap or both are Direct memory.
   */
  @Override
  public boolean requireMatchingBufferType() {
    return true;
  }

  /**
   * Get the minimum compression level.
   * @return The minimum compression level.
   */
  @Override
  public int getMinimumCompressionLevel() {
    return Zstd.minCompressionLevel();
  }

  /**
   * Get the maximum compression level.
   * @return The maximum compression level.
   */
  @Override
  public int getMaximumCompressionLevel() {
    return Zstd.maxCompressionLevel();
  }

  /**
   * Get the default compression level if not set.  Level 0 is the fastest compressor with lower compression ratio.
   * @return The default compression level.
   */
  @Override
  public int getDefaultCompressionLevel() {
    return Zstd.defaultCompressionLevel();
  }

  /**
   * Given the source data size, what is the maximum or worst-case scenario compressed data size?
   * This is an estimate calculation.  The result is usually slightly larger than the sourceDataSize.
   *
   * @param sourceDataSize The source data size.
   * @return The estimated buffer size required to hold the compressed data.
   */
  @Override
  protected int estimateMaxCompressedDataSize(int sourceDataSize) {
    return (int) Zstd.compressBound(sourceDataSize);
  }

  /**
   * Invoke the Zstd compression algorithm given the source data, the compressed destination buffer and its offset.
   * The compression output is stored in the compressed buffer at the specific offset.
   * All parameters have been verified before calling so implementation can skip verification.
   *
   * @param sourceBuffer The source uncompressed data.  It has already been null and empty verified.
   * @param sourceBufferOffset Offset in sourceData to start reading.
   * @param sourceDataSize Number of bytes in sourceData to compress.
   * @param compressedBuffer  The buffer to hold the compressed data.
   * @param compressedBufferOffset The offset in compressedBuffer where the compression output should write.
   * @param compressedDataSize Size of the buffer where compression can write to.  This size should be same as
   *                             estimateMaxCompressedDataSize().
   * @return The size of the compressed data in bytes.
   */
  @Override
  protected int compressNative(ByteBuffer sourceBuffer, int sourceBufferOffset, int sourceDataSize,
      ByteBuffer compressedBuffer, int compressedBufferOffset, int compressedDataSize) throws CompressionException {

    // Zstd requires both buffers type be the same (either both direct memory or both heap), no mixed.
    if (sourceBuffer.isDirect() != compressedBuffer.isDirect()) {
      throw new IllegalArgumentException(
          "Cannot compress due to mismatch sourceBuffer type and compressedBuffer buffer type.");
    }

    // Invoke compress byte array or direct buffer based on buffer type.
    long compressedSize;
    if (sourceBuffer.isDirect()) {
      compressedSize =
          Zstd.compressDirectByteBuffer(compressedBuffer, compressedBufferOffset, compressedDataSize, sourceBuffer,
              sourceBufferOffset, sourceDataSize, getCompressionLevel());
    } else {
      compressedSize =
          Zstd.compressByteArray(compressedBuffer.array(), compressedBuffer.arrayOffset() + compressedBufferOffset,
              compressedDataSize, sourceBuffer.array(), sourceBuffer.arrayOffset() + sourceBufferOffset, sourceDataSize,
              getCompressionLevel());
    }

    // Check for error.
    if (Zstd.isError(compressedSize)) {
      throw new CompressionException(String.format("Zstd compression failed with error code: %d, name: %s. "
              + "sourceBuffer.limit=%d, sourceBufferOffset=%d, sourceDataSize=%d, "
              + "compressedBuffer.capacity=%d, compressedBufferOffset=%d, compressedDataSize=%d, compressionLevel=%d",
          compressedSize, Zstd.getErrorName(compressedSize), sourceBuffer.limit(), sourceBufferOffset, sourceDataSize,
          compressedBuffer.capacity(), compressedBufferOffset, compressedDataSize, getCompressionLevel()));
    }

    return (int) compressedSize;
  }

  /**
   * Invoke the ZStd decompression algorithm given the compressedBuffer and its offset and size.
   * The original data is written to the sourceDataBuffer at the specified offset and size.
   * The size of sourceDataBuffer must be at least the original data size.
   *
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in compressedBuffer where the decompression should start reading.
   * @param compressedDataSize Size of the compressed buffer returned from compressNative().
   * @param decompressedBuffer The buffer to store decompression output (the original source data).
   * @param decompressedBufferOffset Offset where to write the decompressed data.
   * @param decompressedDataSize Size of the buffer to hold the decompressed data.  It should be size of original data.
   */
  @Override
  protected void decompressNative(ByteBuffer compressedBuffer, int compressedBufferOffset, int compressedDataSize,
      ByteBuffer decompressedBuffer, int decompressedBufferOffset, int decompressedDataSize)
      throws CompressionException {

    // Zstd requires both buffers type be the same (either both direct memory or both heap), no mixed.
    if (compressedBuffer.isDirect() != decompressedBuffer.isDirect()) {
      throw new IllegalArgumentException(
          "Cannot decompress due to mismatch sourceBuffer type and compressedBuffer buffer type.");
    }

    // Decompress the buffer either as both direct memory buffer or both heap buffers.
    long decompressedSize;
    if (compressedBuffer.isDirect()) {
      decompressedSize =
          Zstd.decompressDirectByteBuffer(decompressedBuffer, decompressedBufferOffset, decompressedDataSize,
              compressedBuffer, compressedBufferOffset, compressedDataSize);
    } else {
      decompressedSize = Zstd.decompressByteArray(decompressedBuffer.array(),
          decompressedBuffer.arrayOffset() + decompressedBufferOffset, decompressedDataSize, compressedBuffer.array(),
          compressedBuffer.arrayOffset() + compressedBufferOffset, compressedDataSize);
    }

    // Check for error.
    if (Zstd.isError(decompressedSize)) {
      throw new CompressionException(String.format("Zstd decompression failed with error code: %d, name: %s. "
              + "compressedBuffer.limit=%d, compressedBufferOffset=%d, compressedDataSize=%d, "
              + "decompressedBuffer.capacity=%d, decompressedBufferOffset=%d, decompressedDataSize=%d", decompressedSize,
          Zstd.getErrorName(decompressedSize), compressedBuffer.limit(), compressedBufferOffset, compressedDataSize,
          decompressedBuffer.capacity(), decompressedBufferOffset, decompressedDataSize));
    }
  }
}
