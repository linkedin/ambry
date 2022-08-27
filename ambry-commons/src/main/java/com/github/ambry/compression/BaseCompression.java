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

import com.github.ambry.utils.Pair;
import java.nio.charset.StandardCharsets;

/**
 * The base compression algorithm implementation.
 * It contains common algorithm implementation methods.
 */
public abstract class BaseCompression implements Compression {

  // The maximum length of the algorithm name in bytes.
  // This prevents algorithms from using long names that occupy more disk space.
  public static final byte MAX_ALGORITHM_NAME_LENGTH = 10;

  /**
   * The binary representation of the algorithm name.
   * Since this binary is written to every compressed buffer, it caches the binary form for performance purpose.
   */
  private byte[] algorithmNameBinary;

  /**
   * Estimate the worst-case scenario buffer size required to hold the given source data in bytes.
   * The estimated max size is algorithm specific.
   *
   * @param sourceDataSize Size of original data size.
   * @return The estimated worst-case scenario compressed data size in bytes.
   */
  protected abstract int estimateMaxCompressedDataSize(int sourceDataSize);

  /**
   * Invoke the compression algorithm given the source data.  The compression output is stored in the
   * compressed buffer at the specific offset.
   * The compression implementation is algorithm specific.
   *
   * @param sourceData The source uncompressed data.  It has already been null and empty verified.
   * @param compressedBuffer  The buffer to hold the compressed data.
   * @param compressedBufferOffset The offset in compressedBuffer where the compression output should write.
   * @return The size of the compressed data in bytes.
   */
  protected abstract int compress(byte[] sourceData, byte[] compressedBuffer, int compressedBufferOffset);

  /**
   * Invoke the decompression algorithm given the compressedBuffer and the offset of the compression output.
   * The size of sourceDataBuffer is exactly same as the original data source.
   * The decompression implementation is algorithm specific.
   *
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in compressedBuffer where the decompression should start reading.
   * @param sourceDataBuffer The buffer to store decompression output (the original source data).
   */
  protected abstract void decompress(byte[] compressedBuffer, int compressedBufferOffset, byte[] sourceDataBuffer);

  /**
   * Get the algorithm name in binary format.
   * This binary will be saved into the compressed buffer.  The algorithm name will be used to find the decompressor.
   * The byte array contains the size (1 byte) and the algorithm name.
   * The binary generation happens only once per algorithm (cached) and is thread safe.
   *
   * @return Binary presentation of algorithm name.
   */
  private byte[] getAlgorithmNameBinary() {
    if (algorithmNameBinary != null) return algorithmNameBinary;

    synchronized(this) {
      if (algorithmNameBinary == null) {
        String algorithmName = getAlgorithmName();
        if (algorithmName == null || algorithmName.length() == 0) {
          throw new IllegalArgumentException("algorithmName cannot be null or empty.");
        }

        byte[] binary = algorithmName.getBytes(StandardCharsets.UTF_8);
        if (binary.length > MAX_ALGORITHM_NAME_LENGTH) {
          throw new IllegalArgumentException("algorithmName " + algorithmName + " exceeds the size of " +
                  MAX_ALGORITHM_NAME_LENGTH + " bytes.");
        }

        byte[] binaryWithSize = new byte[binary.length + 1];
        binaryWithSize[0] = (byte) binary.length;
        System.arraycopy(binary, 0, binaryWithSize, 1, binary.length);

        algorithmNameBinary = binaryWithSize;
      }
    }

    return algorithmNameBinary;
  }

  /**
   * Given the source data size, what is the maximum compressed buffer size?
   * The max buffer size must sufficient to hold:
   * - algorithm name
   * - original data size
   * - worst-case compression output.
   * This is an estimate calculation it is usually slightly larger than the sourceDataSize.
   *
   * @param sourceDataSize The source data size.
   * @return The estimated buffer size required to hold the compressed data.
   */
  private int getMaxCompressedBufferSize(int sourceDataSize) {
    return estimateMaxCompressedDataSize(sourceDataSize) + getAlgorithmNameBinary().length + 4;
  }

  /**
   * Compress the source data specified in {@code sourceData}.  This method will allocate buffer to store compressed
   * data.  It returns the over-sized buffer and the size of the buffer usage.
   *
   * @param sourceData The source/uncompressed data to compress.  It cannot be null or empty.
   * @return Pair that contains the buffer and the actual size of buffer usage.
   */
  @Override
  public Pair<Integer, byte[]> compress(byte[] sourceData) {
    if (sourceData == null || sourceData.length == 0) {
      throw new IllegalArgumentException("sourceData cannot be null");
    }

    // Allocate sufficient buffer to hold compressed data, name, and original data size.
    int compressedBufferSize = getMaxCompressedBufferSize(sourceData.length);
    byte[] compressedBuffer = new byte[compressedBufferSize];

    // Write algorithm name.
    byte[] algorithmNameBinary = getAlgorithmNameBinary();
    System.arraycopy(algorithmNameBinary, 0, compressedBuffer, 0, algorithmNameBinary.length);
    int offset = algorithmNameBinary.length;

    // Write original source data size.
    int sourceSize = sourceData.length;
    compressedBuffer[offset] = (byte) (sourceSize & 0xFF);
    compressedBuffer[offset + 1] = (byte)(sourceSize >> 8 & 0xFF);
    compressedBuffer[offset + 2] = (byte)(sourceSize >> 16 & 0xFF);
    compressedBuffer[offset + 3] = (byte)(sourceSize >> 24);
    offset += 4;

    // Apply compression and store the output in the remaining buffer.
    // Note: compress() uses less than the remaining buffer and that's why it returns the actual compressed size.
    int compressedDataLength = compress(sourceData, compressedBuffer, offset);

    int actualBufferUsage = compressedDataLength + offset;
    return new Pair<>(actualBufferUsage, compressedBuffer);
  }

  /**
   * Decompress the compressed buffer generated by compress().
   * Note that compressed buffer contains the algorithm name and the original blob size.  This method
   * allocates the exact buffer size to store original data.
   *
   * @param compressedBuffer The compressed data to decompress.  It cannot be null or empty.
   * @return The decompressed data.
   */
  @Override
  public byte[] decompress(byte[] compressedBuffer) {
    if (compressedBuffer == null || compressedBuffer.length < 5 ) {
      throw new IllegalArgumentException("compressedBuffer cannot be null or smaller than 5 bytes.");
    }

    // Assume the algorithm name is correct, so no need to verify.
    int offset = compressedBuffer[0] + 1;
    if (offset + 4 >= compressedBuffer.length) {
      throw new IllegalArgumentException("compressedBuffer too small and does not contain original data size.");
    }

    // Read the original source size.
    int sourceDataSize = (compressedBuffer[offset] & 0xFF) +
        ((compressedBuffer[offset + 1] & 0xFF) << 8) +
        ((compressedBuffer[offset + 2] & 0xFF) << 16) +
        (compressedBuffer[offset + 3] << 24);
    offset += 4;
    byte[] sourceDataBuffer = new byte[sourceDataSize];

    // Call the decompression algorithm to obtain original data.
    decompress(compressedBuffer, offset, sourceDataBuffer);
    return sourceDataBuffer;
  }
}
