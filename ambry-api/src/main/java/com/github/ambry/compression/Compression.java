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
import com.github.ambry.utils.Utils;

/**
 * Compression interface that provides compression and decompression feature.
 * It supports composite/shared memory where caller manages the buffer allocation.
 * It also supports simple APIs that lets the implementation allocate memory from Java heap.
 *
 * <pre>
 * Shared buffer APIs (caller management memory).
 *   Compression example:
 *   {@code
 *     byte[] originalData = ...;
 *     int compressedBufferSize = Compression.getCompressBufferSize(originalData.length);
 *     byte[] compressedBuffer = new byte[compressedBufferSize];
 *     int compressedSize = Compression.compress(originalData, 0, originalData.length,
 *                                   compressedBuffer, 0, compressedBuffer.length);
 *   }
 *
 *   Decompression example:
 *   {@code
 *     byte[] compressedBuffer = ...;
 *     int originalDataSize = Compression.getDecompressBufferSize(compressedBuffer, 0, compressedBuffer.length);
 *     byte[] originalData = new byte[originalDataSize];
 *     int decompressedSize = Compression.decompress(compressedBuffer, 0, compressedBuffer.length,
 *                                   originalData, 0, originalData.length);
 *    }
 *
 * Dedicated buffer APIs (implementation allocate memory).
 *   Compression example:
 *   {@code
 *     byte[] originalData = ...;
 *     Pair&lt;Integer, byte[]&gt; compressedBuffer = Compression.compress(originalData);
 *   }
 *
 *   Decompression example:
 *   {@code
 *     byte[] compressedBuffer = ...;
 *     byte[] originalData = Compression.decompress(compressedBuffer);
 *   }
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
   * contains version, algorithm name, original data size, and compressed data.
   * The compressed buffer may be shared with other buffers, meaning only portion of this buffer was used by
   * compression.  That is why it accepts the offset and size parameters.
   * @param compressedBuffer The compressed buffer.
   * @param compressedBufferOffset The offset in the compressed buffer where compression data is stored.
   * @param compressedDataSize The size in compressedBuffer to read, may not be the size of compressedBuffer.
   * @return The size of original data.
   */
  int getDecompressBufferSize(byte[] compressedBuffer, int compressedBufferOffset, int compressedDataSize)
      throws CompressionException;

  /**
   * Get the original data size stored in compressed buffer.  Compressed buffer is not shared and contains
   * compressed data only.  The compressed buffer is the output of compress() that contains original/source data size.
   * @param compressedBuffer The compressed buffer.  It cannot be null or empty.
   * @return Size of the original data.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  default int getDecompressBufferSize(byte[] compressedBuffer) throws CompressionException {
    Utils.checkNotNullOrEmpty(compressedBuffer, "compressedBuffer cannot be null or empty.");
    return getDecompressBufferSize(compressedBuffer, 0, compressedBuffer.length);
  }

  /**
   * Compress portion of the shared buffer specified in {@code sourceData}.  Both {@code sourceData}
   * and {@code compressedBuffer} can be shared/composite buffers.  That's why both buffers need to provide
   * their size and offset.  The caller is responsible for managing memory allocation.
   *
   * @param sourceBuffer The source/uncompressed data to compress.  It can be a shared buffer.  It cannot be null or
   *                     empty.
   * @param sourceBufferOffset Offset in the sourceBuffer to start reading.
   * @param sourceDataSize The size in bytes in sourceBuffer to read/compress.
   * @param compressedBuffer The compressed buffer where the compressed data is written to.  It can be a shared buffer.
   * @param compressedBufferOffset Offset in compressedBuffer to write to.
   * @param compressedDataSize The maximum size to write inside the compressedBuffer.  Since the compressed size is
   *                           not known, call getCompressBufferSize() for the required size.
   * @return The actual compressed data size in bytes.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  int compress(byte[] sourceBuffer, int sourceBufferOffset, int sourceDataSize,
      byte[] compressedBuffer, int compressedBufferOffset, int compressedDataSize) throws CompressionException;

  /**
   * Compress the source data specified in {@code sourceBuffer}.  The source data is not a shared buffer.
   * This method will allocate buffer to store the compressed data.
   * The returned buffer may be bigger than sourceBuffer in case sourceBuffer is incompressible.
   * The return value contains both the buffer and actual usage in the buffer.
   *
   * @param sourceBuffer The source uncompressed data to compress.  It cannot be null or empty.
   * @return Pair that contains the compressed buffer and the buffer usage size in bytes.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  default Pair<Integer, byte[]> compress(byte[] sourceBuffer) throws CompressionException {
    Utils.checkNotNullOrEmpty(sourceBuffer, "sourceBuffer cannot be null or empty.");
    return compress(sourceBuffer, 0, sourceBuffer.length);
  }

  /**
   * Compress the portion of the shared buffer specified in {@code sourceBuffer}.  This method will allocate buffer to
   * store the compressed data.  The returned buffer may be bigger than sourceBuffer in case sourceBuffer is incompressible.
   * The return value contains both the buffer and actual usage in the buffer.
   *
   * @param sourceBuffer The source uncompressed data to compress.  It cannot be null or empty.
   * @param sourceBufferOffset Offset in sourceBuffer to start reading.
   * @param sourceDataSize Size of source data to read in bytes, not the size of sourceBuffer.
   * @return Pair that contains the compressed buffer and the buffer usage sourceDataSize in bytes.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  default Pair<Integer, byte[]> compress(byte[] sourceBuffer, int sourceBufferOffset, int sourceDataSize)
      throws CompressionException {
    Utils.checkNotNullOrEmpty(sourceBuffer, "sourceBuffer cannot be null or empty.");
    int compressedBufferSize = getCompressBufferSize(sourceDataSize);
    byte[] compressedBuffer = new byte[compressedBufferSize];
    int compressedSize = compress(sourceBuffer, sourceBufferOffset, sourceDataSize, compressedBuffer, 0, compressedBuffer.length);
    return new Pair<>(compressedSize, compressedBuffer);
  }

  /**
   * Decompress portion of the shared buffer in {@code compressedBuffer}.  Both the {@code compressedBuffer} and
   * {@code sourceData} may be shared buffers and that's why both provide their offset and size as parameters.
   * The compressed buffer is the output from compress() that contains the version, algorithm name, original data size,
   * and compressed binary.
   *
   * @param compressedBuffer The buffer that contains compressed data generated by the compress() method.
   * @param compressedBufferOffset Offset in the compressedBuffer to start reading.
   * @param compressedDataSize Number of bytes to read in compressedBuffer.
   * @param decompressedBuffer The buffer to hold the decompressed/original data.  This buffer may be shared.
   * @param decompressedBufferOffset Offset in the sourceData buffer to write.
   * @param decompressedDataSize Size of the original data in bytes, not the size of decompressedBuffer.
   * @return The original data size which is same as number of bytes written to sourceData buffer.
   * @throws CompressionException Throws this exception if decompression has internal failure.
   */
  int decompress(byte[] compressedBuffer, int compressedBufferOffset, int compressedDataSize,
      byte[] decompressedBuffer, int decompressedBufferOffset, int decompressedDataSize) throws CompressionException;

  /**
   * Decompress the compressed buffer.  The compressedBuffer is not shared and contains compressed buffer only.
   * This method allocates and returns the buffer to hold the decompressed data.
   * @param compressedBuffer The compressed buffer generated in compress() method.
   * @return The original/decompressed data.
   * @throws CompressionException Throws this exception if decompression has internal failure.
   */
  default byte[] decompress(byte[] compressedBuffer) throws CompressionException {
    Utils.checkNotNullOrEmpty (compressedBuffer, "compressedBuffer cannot be null or empty.");
    return decompress(compressedBuffer, 0, compressedBuffer.length);
  }

  /**
   * Decompress portion of the compressed buffer.  The compressedBuffer may be shared with other buffers and
   * that's why the compressedBufferOffset and size are provided as parameters.
   * This method allocates and returns the buffer to hold the decompressed data.
   *
   * @param compressedBuffer The compressed buffer generated in compress() method.
   * @param compressedBufferOffset Offset in the compressedBuffer to start reading.
   * @param compressedDataSize Number of bytes to read in compressedBuffer, not the size of compressedBuffer.
   * @return The original/decompressed data.
   * @throws CompressionException Throws this exception if decompression has internal failure.
   */
  default byte[] decompress(byte[] compressedBuffer, int compressedBufferOffset, int compressedDataSize)
      throws CompressionException {
    Utils.checkNotNullOrEmpty (compressedBuffer, "compressedBuffer cannot be null or empty.");
    int originalDataSize = getDecompressBufferSize(compressedBuffer, compressedBufferOffset, compressedDataSize);
    byte[] originalData = new byte[originalDataSize];
    decompress(compressedBuffer, compressedBufferOffset, compressedDataSize,
        originalData, 0, originalData.length);
    return originalData;
  }
}
