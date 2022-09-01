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

/**
 * Compression interface that provides compression and decompression feature.
 * It supports composite/shared memory where caller manages the buffer allocation.
 * It also supports simple APIs that lets the implementation allocate memory from Java heap.
 *
 * Shared buffer APIs (caller management memory).
 *   Compression example:
 *     byte[] originalData = ...;
 *     int compressedBufferSize = Compression.getCompressBufferSize(originalData.length);
 *     byte[] compressedBuffer = new byte[compressedBufferSize];
 *     int compressedSize = Compression.compress(originalData, 0, originalData.length,
 *                                   compressedBuffer, 0, compressedBuffer.length);
 *
 *   Decompression example:
 *     byte[] compressedBuffer = ...;
 *     int originalDataSize = Compression.getDecmopressBufferSize(compressedBuffer, 0, compressedBuffer.length);
 *     byte[] originalData = new byte[originalDataSize];
 *     int decompressedSize = Compression.decompress(compressedBuffer, 0, compressedBuffer.length,
 *                                   originalData, 0, originalData.length);
 *
 * Dedicated buffer APIs (implementation allocate memory).
 *   Compression example:
 *     byte[] originalData = ...;
 *     Pair&lt;Integer, byte[]&gt; compressedBuffer = Compression.compress(originalData);
 *
 *   Decompression example:
 *     byte[] compressedBuffer = ...;
 *     byte[] originalData = Compression.decompress(compressedBuffer);
 */
public interface Compression {

  /**
   * Get the unique compression algorithm name.
   * The name should be short and cannot exceed max size specified in CompressionFactory.
   * The recommended format is Algorithm Name plus the starting version, such as "LZ4_2".
   * Although storing the name as a string takes more bytes than an integer, it's for readability
   * reason in case we need to manually recover data.
   *
   * *********** IMPORTANT/WARNING **********
   * For backward compatibility, NEVER change the algorithm name, even if there is a typo.
   * The name is stored in the compressed data and will be used to automatically locate the
   * decompression algorithm.  If the name has changed, it will fail to find the decompression algorithm.
   *
   * If you're adding a new compression algorithm, use a new unique algorithm name.
   * If you're upgrading the compression library:
   * - use the same name if the upgraded version's decompress() method supports previous version compressed data.
   * - use a new name if the upgrade version's decompress() method does not support previous version compressed data.
   *
   * Example of compatible upgrade:
   *   Say today we use LZ4 1.8 and its name is "LZ4".  One year later, LZ4 1.9 is available.
   *   Since LZ4 1.9 decompress() method supports decompressing LZ4 1.8 and 1.9, leave the name unchanged as "LZ4"
   *   and just upgrade the LZ4 library to 1.9.
   *
   * Example of incompatible upgrade:
   *   Say today we added LZ4 1.8 and its name is "LZ4".  One year later, LZ4 2.0 is released, but LZ4 2.0 is
   *   incompatible with 1.8.  That means LZ4 2.0 cannot data compressed with L4 1.8.
   *   In this case, introduce a new name like "LZ4_2" while keeping LZ4 1.8.
   *
   * @return A unique name of this compression algorithm.
   */
  String getAlgorithmName();

  /**
   * Calculate and return the size of buffer required to call compress().
   * The caller is expected to allocate buffer of this size before calling compress().
   *
   * @param sourceDataSize The size of source data.
   * @return The estimated buffer size required to store the compressed buffer in worst-case scenario.
   */
  int getCompressBufferSize(int sourceDataSize);

  /**
   * Get the original data size stored inside the shared/composite compressed buffer.  The compressed buffer
   * contains version, algorithm name, original data size, and compressed data.
   * The compressed buffer may be shared with other buffers, meaning only portion of this buffer was used by
   * compression.  That is why it accepts the offset and size parameters.
   * @param compressedBuffer The compressed buffer.
   * @param offset The offset in the compressed buffer where compression data is stored.
   * @param size The size in compressedBuffer to read.
   * @return The size of original data.
   */
  int getDecompressBufferSize(byte[] compressedBuffer, int offset, int size);

  /**
   * Get the original data size stored in compressed buffer.  Compressed buffer is not shared and contains
   * compressed data only.  The compressed buffer is the output of compress() that contains original/source data size.
   * @param compressedBuffer The compressed buffer.
   * @return Size of the original data.
   */
  default int getDecompressBufferSize(byte[] compressedBuffer) {
    if (compressedBuffer == null) throw new IllegalArgumentException("compressedBuffer cannot be null.");
    return getDecompressBufferSize(compressedBuffer, 0, compressedBuffer.length);
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
  int compress(byte[] sourceData, int sourceDataOffset, int sourceDataSize,
      byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize);

  /**
   * Compress the source data specified in {@code sourceData}.  The source data is not a shared buffer.
   * This method will allocate buffer to store the compressed data.
   * The returned buffer may be bigger than sourceData in case sourceData is incompressible.
   * The return value contains both the buffer and actual usage in the buffer.
   *
   * @param sourceData The source uncompressed data to compress.  It cannot be null or empty.
   * @return Pair that contains the compressed buffer and the buffer usage size in bytes.
   */
  default Pair<Integer, byte[]> compress(byte[] sourceData) {
    if (sourceData == null) throw new IllegalArgumentException("sourceData cannot be null.");
    return compress(sourceData, 0, sourceData.length);
  }

  /**
   * Compress the portion of the shared buffer specified in {@code sourceData}.  This method will allocate buffer to
   * store the compressed data.  The returned buffer may be bigger than sourceData in case sourceData is incompressible.
   * The return value contains both the buffer and actual usage in the buffer.
   *
   * @param sourceData The source uncompressed data to compress.  It cannot be null or empty.
   * @param offset Offset of source data to start reading.
   * @param size Size of source data to read in bytes.
   * @return Pair that contains the compressed buffer and the buffer usage size in bytes.
   */
  default Pair<Integer, byte[]> compress(byte[] sourceData, int offset, int size) {
    int compressedBufferSize = getCompressBufferSize(size);
    byte[] compressedBuffer = new byte[compressedBufferSize];
    int compressedSize = compress(sourceData, offset, size, compressedBuffer, 0, compressedBuffer.length);
    return new Pair<>(compressedSize, compressedBuffer);
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
  int decompress(byte[] compressedBuffer, int compressedBufferOffset, int compressedBufferSize,
      byte[] sourceData, int sourceDataOffset, int sourceDataSize);

  /**
   * Decompress the compressed buffer.  The compressedBuffer is not shared and contains compressed buffer only.
   * This method allocates and returns the buffer to hold the decompressed data.
   * @param compressedBuffer The compressed buffer generated in compress() method.
   * @return The original/decompressed data.
   */
  default byte[] decompress(byte[] compressedBuffer) {
    if (compressedBuffer == null) throw new IllegalArgumentException("compressedBuffer cannot be null.");
    return decompress(compressedBuffer, 0, compressedBuffer.length);
  }

  /**
   * Decompress portion of the compressed buffer.  The compressedBuffer may be shared with other buffers and
   * that's why the offset and size are provided as parameters.
   * This method allocates and returns the buffer to hold the decompressed data.
   *
   * @param compressedBuffer The compressed buffer generated in compress() method.
   * @param offset Offset in the compressedBuffer to start reading.
   * @param size Number of bytes to read in compressedBuffer.
   * @return The original/decompressed data.
   */
  default byte[] decompress(byte[] compressedBuffer, int offset, int size) {
    int originalDataSize = getDecompressBufferSize(compressedBuffer, offset, size);
    byte[] sourceData = new byte[originalDataSize];
    decompress(compressedBuffer, offset, size, sourceData, 0, sourceData.length);
    return sourceData;
  }

}
