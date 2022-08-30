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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * The LZ4 compression algorithm.  LZ4 has very fast decompression speed.
 * More info about LZ4  <a href="https://github.com/lz4/lz4">GitHub</a>.
 *
 * LZ4 has 3 implementations and LZ4Factory.fastestInstance() picks the implementation in this order
 * 1. Native with JNI.
 * 2. Pure Java unsafe instance
 * 3. Pure Java safe instance
 *
 * Each LZ4 compressor offers 2 compressors.
 * - LZ4 fast compressor (uses about 16 KB memory) for fastest compression speed but lower compression ratio.
 *   To use fast compressor, set compression level to 0.
 * - LZ4 high compressor (HC) (uses about 256 KB memory) for higher compression ratio but about 10x slower.
 *   To use LZ4 HC, select a compression level between 1 and 17.  Higher compression level yields higher compression
 *   ratio but slower compression speed.
 *
 * The decompressor can decompress data compressed in either compressors and the decompression speed is about the
 * same regardless of which compression level was used.
 */
public class LZ4Compression extends BaseCompressionWithLevel {

  // For performance purpose, cache the factory.
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();

  /**
   * Get the unique name of this compression algorithm.
   * WARNING - Do not change the algorithm name.  See Compression interface for detail.
   * @return Name of this algorithm.
   */
  @Override
  public String getAlgorithmName() { return "LZ4"; }

  /**
   * Get the minimum compression level.
   * @return The minimum compression level.
   */
  @Override
  public int getMinimumCompressionLevel() { return 0; }

  /**
   * Get the maximum compression level.
   * @return The maximum compression level.
   */
  @Override
  public int getMaximumCompressionLevel() { return 17; }

  /**
   * Get the default compression level if not set.  Level 0 is the fastest compressor with lower compression ratio.
   * @return The default compression level.
   */
  @Override
  public int getDefaultCompressionLevel() { return 0; }

  /**
   * Get the compressor based on the compressor level.
   * @return The LZ4 compressor.
   */
  private LZ4Compressor getCompressor() {
    if (getCompressionLevel() == 0) return LZ4_FACTORY.fastCompressor();
    return LZ4_FACTORY.highCompressor(getCompressionLevel());
  }

  /**
   * Get the decompressor based on the compressor level.  For LZ4, the fast decompressor works for all levels.
   * @return The decompressor.
   */
  private LZ4FastDecompressor getDecompressor() {
    return LZ4_FACTORY.fastDecompressor();
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
    return getCompressor().maxCompressedLength(sourceDataSize);
  }

  /**
   * Invoke the LZ4 compression algorithm given the source data, the compressed destination buffer and its offset.
   *
   * @param sourceData The source data.  It has already null and empty verified.
   * @param compressedBuffer  The buffer to hold the compressed data.
   * @param compressedBufferOffset The offset in compressedBuffer where the compressed data should write.
   * @return The size of the compressed data in bytes.
   */
  @Override
  protected int compress(byte[] sourceData, byte[] compressedBuffer, int compressedBufferOffset) {
    try {
      return getCompressor().compress(sourceData, 0, sourceData.length, compressedBuffer, compressedBufferOffset,
          compressedBuffer.length - compressedBufferOffset);
    }
    catch (Exception ex) {
      throw new CompressionException("LZ4 compression failed.", ex);
    }
  }

  /**
   * Invoke the LZ4 decompression algorithm given the compressedBuffer and its offset.
   * The sourceDataBuffer has the exact same size as the original data source.
   *
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in compressedBuffer where the decompression should start reading.
   * @param sourceDataBuffer The buffer to hold the original source data.
   */
  @Override
  protected void decompress(byte[] compressedBuffer, int compressedBufferOffset, byte[] sourceDataBuffer) {
    // This decompressor supports all compressors, LZ4 and LZ4 HC.
    try {
      getDecompressor().decompress(compressedBuffer, compressedBufferOffset,
          sourceDataBuffer, 0, sourceDataBuffer.length);
    }
    catch (Exception ex) {
      throw new CompressionException("LZ4 decompression failed.", ex);
    }
  }
}
