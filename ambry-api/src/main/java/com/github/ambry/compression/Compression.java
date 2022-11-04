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

import java.nio.ByteBuffer;


/**
 * Compression interface that provides compression and decompression feature.
 *
 * <pre>
 *   Compression example:
 *   {@code
 *     ByteBuffer originalData = ...;
 *     int compressedBufferSize = Compression.getCompressBufferSize(originalData.remaining());
 *     ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(compressedBufferSize);
 *     int compressedSize = Compression.compress(originalData, compressedBuffer);
 *   }
 *
 *   Decompression example:
 *   {@code
 *     ByteBuffer compressedBuffer = ...;
 *     int originalDataSize = Compression.getDecompressBufferSize(compressedBuffer);
 *     ByteBuffer originalData = ByteBuffer.allocateDirect(originalDataSize);
 *     int decompressedSize = Compression.decompress(compressedBuffer, originalData);
 *    }
 * </pre>
 */
public interface Compression {

  /**
   * Get the unique compression algorithm name.
   * The name should be short and cannot exceed max size specified in CompressionMap.
   * The recommended format is Algorithm Name plus the starting version, such as "LZ4_2".
   * Although storing the name as a string takes more bytes than an integer, it's for readability
   * reason in case we need to manually recover data.
   * <p>
   * *********** IMPORTANT/WARNING **********
   * For backward compatibility, NEVER change the algorithm name, even if there is a typo.
   * The name is stored in the compressed data and will be used to automatically locate the
   * decompression algorithm.  If the name has changed, it will fail to find the decompression algorithm.
   * <p>
   * If you're adding a new compression algorithm, use a new unique algorithm name.
   * If you're upgrading the compression library:
   * - use the same name if the upgraded version's decompress() method supports previous version compressed data.
   * - use a new name if the upgrade version's decompress() method does not support previous version compressed data.
   * <p>
   * Example of compatible upgrade:
   *   Say today we use LZ4 1.8 and its name is "LZ4".  One year later, LZ4 1.9 is available.
   *   Since LZ4 1.9 decompress() method supports decompressing LZ4 1.8 and 1.9, leave the name unchanged as "LZ4"
   *   and just upgrade the LZ4 library to 1.9.
   * <p>
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
   * contains version, algorithm name, original data size, and compressed data.  This method helps to determine
   * the decompressed buffer size before calling decompress().
   * This method does not change the index of compressedBuffer.
   *
   * @param compressedBuffer The compressed buffer.  The buffer must be position to the beginning of buffer.
   *                         The indexes are not affected and remain unchanged.
   * @return The size of original data.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  int getDecompressBufferSize(ByteBuffer compressedBuffer) throws CompressionException;

  /**
   * Some compression algorithms, such as ZStd, are picky on the source and destination buffer types.
   * In Zstd, either both buffers are Heap or both are Direct memory.
   * In LZ4, any combination of Heap and Direct memory are acceptable.
   * This method indicate whether this compression algorithm requires both source and destination buffer type to match.
   */
  boolean requireMatchingBufferType();

  /**
   * Compress the buffer specified in {@code sourceData}.  The entire sourceBuffer will be read,
   * so set the position and limit to the range of data to read in sourceBuffer before calling this method.
   * After calling this method, sourceBuffer position will be advanced to the buffer limit,
   * and the compressedBuffer position will be advanced to the end of the compressed binary.
   *
   * @param sourceBuffer The source/uncompressed data to compress.  It cannot be null or empty.
   * @param compressedBuffer The compressed buffer where the compressed data will be written to.  Its size must be
   *                       at least the size return from getCompressBufferSize() or it may fail due to buffer size.
   * @return The actual compressed data size in bytes.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  int compress(ByteBuffer sourceBuffer, ByteBuffer compressedBuffer) throws CompressionException;

  /**
   * Decompress the buffer specified in {@code compressedBuffer}.  Since compressedBuffer structure contains the
   * original data size, it reads only the portion required to decompress.
   * After calling, compressedBuffer position will be advanced to right after the compressed binary, and the
   * decompressedBuffer position will be advanced to the end of the decompressed binary.
   *
   * @param compressedBuffer The buffer that contains compressed data generated by the compress() method.
   * @param decompressedBuffer The buffer to hold the decompressed/original data.  The size should be at least the
   *                           size returned by getDecompressBufferSize().
   * @return The original data size which is same as number of bytes written to sourceData buffer.
   * @throws CompressionException Throws this exception if decompression has internal failure.
   */
  int decompress(ByteBuffer compressedBuffer, ByteBuffer decompressedBuffer) throws CompressionException;
}
