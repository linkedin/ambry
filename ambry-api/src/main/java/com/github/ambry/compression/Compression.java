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
   * Compress the source data specified in {@code sourceData}.  This method will allocate buffer to store the
   * compressed data, the name, and original data size.  The returned buffer may be bigger than sourceData
   * in case sourceData is incompressible.  The return value contains both the buffer and actual usage in the buffer.
   *
   * The compressed buffer format:
   * - Algorithm name size (1 byte) - size of AlgorithmName in bytes.
   * - Algorithm name (N bytes) - name of compression algorithm, up to the max size specified in CompressionFactory.
   * - Original Data size (4 bytes) - up to 2 GB.
   * - Compressed data - the output of the compression algorithm.
   *
   * Note that the compressed data does not contain original data size, but decompress() requires the original data
   * size.  The original data size is stored as 4-bytes integer in the compressed buffer.
   *
   * @param sourceData The source uncompressed data to compress.  It cannot be null or empty.
   * @return Pair that contains the compressed buffer and the buffer usage size in bytes.
   */
  Pair<Integer, byte[]> compress(byte[] sourceData);

  /**
   * Decompress the compressed data specified in {@code compressedBuffer}.  The compressed buffer contains the
   * algorithm name, original data size, and compression binary so decompress() will allocate the exact buffer size
   * and find the appropriate decompression algorithm by name.
   *
   * @param compressedBuffer The compressed buffer generated in compress() method.
   * @return The original decompressed data.
   */
  byte[] decompress(byte[] compressedBuffer);
}
