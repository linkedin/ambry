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

/**
 * Zstandard compression algorithm using Zstandard native and JNI.
 * Zstandard has fast decompression speed and pretty good compression ratio.
 * It is not as fast as LZ4 but has higher compression ratio than LZ4.
 * More info about Zstandard in <a href="https://github.com/lz4/lz4">GitHub</a>.
 *
 * Zstd compression level range is from negative 7 (fastest) to 22 (slowest in compression speed,
 * but best compression ratio), with level 3 as the default.
 */
public class ZstdCompression extends BaseCompressionWithLevel {

  /**
   * Get the unique name of this compression algorithm.
   * WARNING - Do not change the algorithm name.  See Compression interface for detail.
   * @return Name of this algorithm.
   */
  @Override
  public String getAlgorithmName() {
    return "ZSTD";
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
   *
   * @param sourceData The source data.  It has already null and empty verified.
   * @param compressedBuffer  The buffer to hold the compressed data.
   * @param compressedBufferOffset The offset in compressedBuffer where the compressed data should write.
   * @return The size of the compressed data in bytes.
   */
  @Override
  protected int compress(byte[] sourceData, byte[] compressedBuffer, int compressedBufferOffset) {
    long compressedSize = Zstd.compressByteArray(compressedBuffer, compressedBufferOffset,
        compressedBuffer.length - compressedBufferOffset, sourceData, 0, sourceData.length,
        getCompressionLevel());

    if (Zstd.isError(compressedSize)) {
      throw new CompressionException("ZStd compression failed with error code " + compressedSize + ", name: "
          + Zstd.getErrorName(compressedSize));
    }
    return (int) compressedSize;
  }

  /**
   * Invoke the ZStd decompression algorithm given the compressedBuffer and its offset.
   * The sourceDataBuffer has the exact same size as the original data source.
   *
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in compressedBuffer where the decompression should start reading.
   * @param sourceDataBuffer The buffer to hold the original source data.
   */
  @Override
  protected void decompress(byte[] compressedBuffer, int compressedBufferOffset, byte[] sourceDataBuffer) {
    long decompressedSize = Zstd.decompressByteArray(sourceDataBuffer, 0, sourceDataBuffer.length,
        compressedBuffer, compressedBufferOffset,compressedBuffer.length - compressedBufferOffset);

    if (Zstd.isError(decompressedSize)) {
      throw new CompressionException("ZStd decompression failed with error code " + decompressedSize + ", name: "
          + Zstd.getErrorName(decompressedSize));
    }

    if (decompressedSize != sourceDataBuffer.length) {
      throw new CompressionException("ZStd decompression completed but the decompressed size and original size mismatch.");
    }
  }
}