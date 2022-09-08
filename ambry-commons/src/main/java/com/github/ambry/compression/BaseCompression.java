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
   * Invoke the native compression algorithm given the source data.  The compression output is stored in the
   * compressed buffer at the specific offset.
   * The compression implementation is algorithm specific.
   * All parameters have been verified before calling so implementation can skip verification.
   *
   * @param sourceData The source uncompressed data.  It has already been null and empty verified.
   * @param sourceDataOffset Offset in sourceData to start reading.
   * @param sourceDataSize Number of bytes in sourceData to compress.
   * @param compressedBuffer  The buffer to hold the compressed data.
   * @param compressedBufferOffset The offset in compressedBuffer where the compression output should write.
   * @param compressedBufferSize Size of the buffer where compression can write to.  This size should be same as
   *                             estimateMaxCompressedDataSize().
   * @return The size of the compressed data in bytes.
   */
  protected abstract int compressNative(byte[] sourceData, int sourceDataOffset, int sourceDataSize,
      byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize);

  /**
   * Invoke the native decompression algorithm given the compressedBuffer and the offset of the compression output.
   * The size of sourceDataBuffer must be at least the original data source size.
   * The decompression implementation is algorithm specific.
   *
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in compressedBuffer where the decompression should start reading.
   * @param compressedBufferSize Size of the compressed buffer returned from compressNative().
   * @param sourceDataBuffer The buffer to store decompression output (the original source data).
   * @param sourceDataOffset Offset where to write the decompressed data.
   * @param sourceDataSize Size of the buffer to hold the decompressed data.  It should be size of original data.
   */
  protected abstract void decompressNative(byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize,
      byte[] sourceDataBuffer, int sourceDataOffset, int sourceDataSize);

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
   * - version (1 byte)
   * - algorithm name binary.
   * - original data size (4 bytes)
   * - worst-case compression output.
   * This is an estimate calculation it is usually slightly larger than the sourceDataSize.
   *
   * @param sourceDataSize The source data size.
   * @return The estimated buffer size required to hold the compressed data.
   */
  public int getCompressBufferSize(int sourceDataSize) {
    return estimateMaxCompressedDataSize(sourceDataSize) + getAlgorithmNameBinary().length + 5;
  }

  /**
   * Get the original data size stored inside the shared/composite compressed buffer.  The compressed buffer
   * contains version, algorithm name, original data size, and compressed data.
   * The compressed buffer may be shared with other buffers, meaning only portion of this buffer was used by
   * compression.  That is why it accepts the offset and size parameters.
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in the compressed buffer where compression data is stored.
   * @param compressedBufferSize The size in compressedBuffer to read.
   * @return The size of original data.
   */
  public int getDecompressBufferSize(byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize) {
    // Verify each parameter.
    verifyCompressedBuffer(compressedBuffer, compressedBufferOffset, compressedBufferSize);
    if (compressedBufferSize < 6) {
      throw new IllegalArgumentException("compressedBufferSize is " + compressedBufferSize + " is too small.");
    }

    // Check version.  For now, only version 1 is supported.  Modify this code when more version added.
    byte version = compressedBuffer[compressedBufferOffset];
    if (version != 1) {
      // It should throw CompressionException to indicate internal error instead of illegal argument because
      // the compressed has a version that is not supported.  It's probably a code problem, not data problem.
      throw new CompressionException("compressedBuffer has unsupported version " + version);
    }

    // Assume the algorithm name is correct, so skip the name verification.
    // Original size offset = 1 (version) + 1 (name size) + name length;
    int offset = compressedBufferOffset + 2 + compressedBuffer[compressedBufferOffset + 1];
    if (offset + 4 >= compressedBuffer.length || offset + 4 >= compressedBufferSize) {
      throw new IllegalArgumentException("compressedBuffer too small and does not contain original data size.");
    }

    // Read the original source size.
    return (compressedBuffer[offset] & 0xFF) +
        ((compressedBuffer[offset + 1] & 0xFF) << 8) +
        ((compressedBuffer[offset + 2] & 0xFF) << 16) +
        (compressedBuffer[offset + 3] << 24);
  }

  /**
   * Compress portion of the shared buffer specified in {@code sourceData}.  Both {@code sourceData}
   * and {@code compressedBuffer} can be shared/composite buffers.  That's why both buffers need to provide
   * their size and offset.  The caller is responsible for managing memory allocation.
   *
   * The compressed buffer is a structure in this format:
   * - Version (1 byte) - the structure/format version.  It starts with value '1'.
   * - Algorithm name size (1 byte) - size of AlgorithmName in bytes.
   * - Algorithm name (N bytes) - name of compression algorithm, up to the max size specified in CompressionFactory.
   * - Original Data size (4 bytes) - up to 2 GB.
   * - Compressed data - the output of the compression algorithm.
   *
   * Note that the compressed data does not contain original data size, but decompress() requires the original data
   * size.  The original data size is stored as 4-bytes integer in the compressed buffer.
   *
   * @param sourceData The source uncompressed data to compress.  It cannot be null or empty.
   * @param sourceDataOffset Offset in the sourceData to start compressing.
   * @param sourceDataSize The size to compress.
   * @param compressedBuffer The compressed buffer where the compressed data is written to.
   * @param compressedBufferOffset Offset in compressedBuffer to write to.
   * @param compressedBufferSize The maximum size to write inside the compressedBuffer.
   * @return The actual compressed data size in bytes.
   */
  @Override
  public int compress(byte[] sourceData, int sourceDataOffset, int sourceDataSize,
      byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize) {
    verifySourceData(sourceData, sourceDataOffset, sourceDataSize);
    verifyCompressedBuffer(compressedBuffer, compressedBufferOffset, compressedBufferSize);

    int overheadSize = 5 + getAlgorithmNameBinary().length;
    if (compressedBufferSize < overheadSize) {
      throw new IllegalArgumentException("compressedBufferSize " + compressedBufferSize + " is too small.");
    }

    // Write version #.
    int offset = compressedBufferOffset;
    compressedBuffer[offset] = 1;
    offset++;

    // Write algorithm name.
    byte[] algorithmNameBinary = getAlgorithmNameBinary();
    System.arraycopy(algorithmNameBinary, 0, compressedBuffer, offset, algorithmNameBinary.length);
    offset += algorithmNameBinary.length;

    // Write original source data size.
    int sourceSize = sourceData.length;
    compressedBuffer[offset] = (byte) (sourceSize & 0xFF);
    compressedBuffer[offset + 1] = (byte)(sourceSize >> 8 & 0xFF);
    compressedBuffer[offset + 2] = (byte)(sourceSize >> 16 & 0xFF);
    compressedBuffer[offset + 3] = (byte)(sourceSize >> 24);
    offset += 4;

    // Apply compression and store the output in the remaining buffer.
    // Note: compress() uses less than the remaining buffer and that's why it returns the actual compressed size.
    int compressedDataLength = compressNative(sourceData, sourceDataOffset, sourceDataSize,
        compressedBuffer, offset, compressedBufferSize - overheadSize);

    return (overheadSize + compressedDataLength);
  }

  private void verifySourceData(byte[] sourceData, int sourceDataOffset, int sourceDataSize) {
    if (sourceData == null || sourceData.length == 0) {
      throw new IllegalArgumentException("sourceData cannot be null");
    }
    if (sourceDataOffset < 0 || sourceDataOffset >= sourceData.length) {
      throw new IllegalArgumentException("sourceDataOffset is outside of sourceData.");
    }
    if (sourceDataSize < 0 || sourceDataOffset + sourceDataSize > sourceData.length) {
      throw new IllegalArgumentException("sourceDataSize is outside of sourceData.");
    }
  }

  /**
   * Verify compressed buffer parameters.
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset  The compressed buffer offset.
   * @param compressedBufferSize The compressed buffer size.
   */
  private void verifyCompressedBuffer(byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize) {
    // Verify target buffer parameters.
    if (compressedBuffer == null || compressedBuffer.length == 0) {
      throw new IllegalArgumentException("compressedBuffer cannot be null or empty.");
    }
    if (compressedBufferOffset < 0 || compressedBufferOffset >= compressedBuffer.length) {
      throw new IllegalArgumentException("compressedBufferOffset is outside of compressedBuffer.");
    }
    if (compressedBufferSize < 0 || compressedBufferOffset + compressedBufferSize > compressedBuffer.length) {
      throw new IllegalArgumentException("compressedBufferSize " + compressedBufferSize + " is invalid.");
    }
  }

  /**
   * Decompress portion of the shared buffer in {@code compressedBuffer}.  Both the {@code compressedBuffer} and
   * {@code sourceData} may be shared buffers and that's why both provide their offset and size as parameters.
   * The compressed buffer is the output from compress() that contains the version, algorithm name, original data size,
   * and compressed binary.
   *
   * @param compressedBuffer The compressed buffer generated in compress() method.
   * @param compressedBufferOffset Offset in the compressedBuffer to start reading.
   * @param compressedBufferSize Number of bytes to read in compressedBuffer.
   * @param sourceData The buffer to hold the decompressed/original data.
   * @param sourceDataOffset Offset in the sourceData buffer to write.
   * @param sourceDataSize Size of the original data in bytes.
   * @return The original data size which is same as number of bytes written to sourceData buffer.
   */
  @Override
  public int decompress(byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize,
      byte[] sourceData, int sourceDataOffset, int sourceDataSize) {
    // Verify source data parameters.
    verifySourceData(sourceData, sourceDataOffset, sourceDataSize);

    // Get the original size from the compressed buffer.  It also verifies compressed buffer parameters.
    int originalSize = getDecompressBufferSize(compressedBuffer, compressedBufferOffset, compressedBufferSize);
    if (sourceDataSize < originalSize) {
      throw new IllegalArgumentException("sourceData size " + sourceDataSize +
          " is too small to hold decompressed data size " + originalSize);
    }

    // Calculate overhead size = 1 (version) + 1 (name size) + name length + 4 (original size);
    int overheadSize = compressedBufferOffset + 6 + compressedBuffer[compressedBufferOffset + 1];
    decompressNative(compressedBuffer, compressedBufferOffset + overheadSize,
        compressedBufferSize - overheadSize, sourceData, sourceDataOffset, originalSize);
    return originalSize;
  }
}
