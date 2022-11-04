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

import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * The base compression algorithm implementation.
 * It contains common algorithm implementation methods.
 */
public abstract class BaseCompression implements Compression {

  /**
   * The maximum length of the algorithm name in bytes.
   * This prevents algorithms from using long names that occupy more disk space.
   */
  public static final int MAX_ALGORITHM_NAME_LENGTH = 10;

  /**
   * The minimal overhead size of the compressed data structure in bytes.
   * The compressed data structure contains:
   * - 1 byte version
   * - 1 byte name size
   * - N bytes name characters
   * - 4 bytes original size.
   * - N bytes compressed data.
   * Excluding the name chars, the minimal size is 6 bytes.
   */
  private static final int VERSION_SIZE = 1;
  private static final int VERSION_AND_ORIGINAL_SIZE_SIZE = 5;
  private static final int ALGORITHM_NAME_LENGTH_SIZE = 1;
  private static final int VERSION_AND_NAME_LENGTH_AND_ORIGINAL_SIZE_SIZE =
      VERSION_AND_ORIGINAL_SIZE_SIZE + ALGORITHM_NAME_LENGTH_SIZE;

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
   * compressed buffer at the specific offset.  The compression implementation is algorithm specific.
   * This method may or may not alter the indexes of sourceBuffer and compressedBuffer, so set the indexes after calling.
   * Assume all parameters have been verified before calling so implementation can skip verification.
   *
   * @param sourceBuffer The source uncompressed data.  This buffer may be shared.
   *                     It has already been null and empty verified.
   * @param sourceBufferOffset Offset in sourceBuffer to start reading.
   * @param sourceDataSize Number of bytes in sourceBuffer to compress, not the size of sourceBuffer.
   * @param compressedBuffer  The buffer to hold the compressed data.  This buffer may be shared.
   * @param compressedBufferOffset The offset in compressedBuffer where the compression output should write.
   * @param compressedDataSize Size in bytes in compressedBuffer reserved for compression to write.
   *                         This size should be same as estimateMaxCompressedDataSize().
   * @return The size of the actual compressed data in bytes.
   */
  protected abstract int compressNative(ByteBuffer sourceBuffer, int sourceBufferOffset, int sourceDataSize,
      ByteBuffer compressedBuffer, int compressedBufferOffset, int compressedDataSize) throws CompressionException;

  /**
   * Invoke the native decompression algorithm given the compressedBuffer and the offset of the compression output.
   * The size of sourceDataBuffer must be at least the original data source size.
   * The decompression implementation is algorithm specific.
   * This method may or may not alter the indexes of compressedBuffer and compressedBuffer, so set the indexes after calling.
   *
   * @param compressedBuffer The compressed buffer.  This buffer may be shared.
   * @param compressedBufferOffset The offset in compressedBuffer where the decompression should start reading.
   * @param compressedDataSize Size of the compressed data returned from compressNative(), not size of compressedBuffer.
   * @param decompressedBuffer The buffer to store the decompressed output (the original data).
   * @param decompressedBufferOffset Offset in decompressedBuffer to start writing the decompressed data.
   * @param decompressedDataSize Size of the buffer to hold the decompressed data in decompressedBuffer.
   *                           It should be same as the original data size.
   */
  protected abstract void decompressNative(ByteBuffer compressedBuffer, int compressedBufferOffset,
      int compressedDataSize, ByteBuffer decompressedBuffer, int decompressedBufferOffset, int decompressedDataSize)
      throws CompressionException;

  /**
   * Get the algorithm name in binary format.
   * This binary will be saved into the compressed buffer.  The algorithm name will be used to find the decompressor.
   * The byte array contains the algorithm name.
   * The binary generation happens only once per algorithm (cached) and is thread safe.
   *
   * @return Binary presentation of algorithm name.  Its length cannot exceed MAX_ALGORITHM_NAME_LENGTH bytes.
   */
  private byte[] getAlgorithmNameBinary() {
    if (algorithmNameBinary != null) {
      return algorithmNameBinary;
    }

    synchronized (this) {
      if (algorithmNameBinary == null) {
        String algorithmName = getAlgorithmName();
        if (algorithmName == null || algorithmName.length() == 0) {
          throw new IllegalArgumentException("algorithmName cannot be null or empty.");
        }

        // Convert name to binary.
        byte[] binary = algorithmName.getBytes(StandardCharsets.UTF_8);
        if (binary.length > MAX_ALGORITHM_NAME_LENGTH) {
          throw new IllegalArgumentException(
              "algorithmName " + algorithmName + " exceeds the size of " + MAX_ALGORITHM_NAME_LENGTH + " bytes.");
        }
        algorithmNameBinary = binary;
      }
    }

    return algorithmNameBinary;
  }

  /**
   * Given the source data size, what is the maximum compressed buffer size?
   * The max buffer size must sufficient to hold the compressed data structure.
   * This is an estimate calculation it is usually slightly larger than the sourceDataSize.
   *
   * @param sourceDataSize The source data size.
   * @return The estimated buffer size required to hold the compressed data.
   */
  public int getCompressBufferSize(int sourceDataSize) {
    return VERSION_AND_NAME_LENGTH_AND_ORIGINAL_SIZE_SIZE + getAlgorithmNameBinary().length
        + estimateMaxCompressedDataSize(sourceDataSize);
  }

  /**
   * Get the original data size stored inside the compressed buffer.  The compressed buffer contains version,
   * algorithm name, original data size, and compressed data.  The compressedBuffer indexes will not be changed.
   * @param compressedBuffer The compressed buffer.
   * @return The size of original data.
   */
  public int getDecompressBufferSize(ByteBuffer compressedBuffer) throws CompressionException {
    Utils.checkNotNullOrEmpty(compressedBuffer, "compressedBuffer cannot be null or empty.");
    if (compressedBuffer.remaining() < VERSION_AND_NAME_LENGTH_AND_ORIGINAL_SIZE_SIZE) {
      throw new IllegalArgumentException(
          "compressedDataSize of " + compressedBuffer.remaining() + " bytes is too small.");
    }

    // Check version.  For now, only version 1 is supported.  Modify this code when more version added.
    compressedBuffer.mark();
    try {
      byte version = compressedBuffer.get();
      if (version != 1) {
        // It throws CompressionException to indicate internal error instead of illegal argument because
        // the compressed buffer contains a version that is not supported.  It's probably a code problem, not data problem.
        // Modify this condition as new versions are added.
        throw new CompressionException("compressedBuffer has unsupported version " + version);
      }

      // Assume the algorithm name is correct, so skip the name verification.
      // Original size offset = version (1 byte) + name size (1 byte) + name (N byte) + data size (4 bytes);
      int nameSizeInBytes = compressedBuffer.get();
      int dataSizePosition = compressedBuffer.position() + nameSizeInBytes;
      if (dataSizePosition + 4 >= compressedBuffer.limit()) {
        throw new IllegalArgumentException("compressedBuffer too small and does not contain original data size.");
      }
      compressedBuffer.position(dataSizePosition);
      return compressedBuffer.getInt();
    } finally {
      compressedBuffer.reset();
    }
  }

  /**
   * Compress the buffer specified in {@code sourceBuffer}.  The entire sourceBuffer will be read,
   * so set the position and limit to the range of data to read in sourceBuffer before calling this method.
   * After calling, sourceBuffer position will be advanced to the buffer limit, and the compressedBuffer position
   * will be advanced to the end of the compressed binary.
   * If compression failed, the sourceBuffer position will not be advanced.
   *
   * @param sourceBuffer The source/uncompressed data to compress.  It cannot be null or empty.
   * @param compressedBuffer The compressed buffer where the compressed data will be written to.  Its size must be
   *                       at least the size return from getCompressBufferSize() or it may fail due to buffer size.
   * @return The actual compressed data size in bytes.
   * @throws CompressionException Throws this exception if compression has internal failure.
   */
  @Override
  public int compress(ByteBuffer sourceBuffer, ByteBuffer compressedBuffer) throws CompressionException {
    Utils.checkNotNullOrEmpty(sourceBuffer, "sourceBuffer cannot be null or empty.");
    Utils.checkNotNullOrEmpty(compressedBuffer, "compressedBuffer cannot be null or empty.");

    int overheadSize = VERSION_AND_NAME_LENGTH_AND_ORIGINAL_SIZE_SIZE + getAlgorithmNameBinary().length;
    if (compressedBuffer.remaining() < overheadSize) {
      throw new IllegalArgumentException("compressedBuffer " + compressedBuffer.remaining() + " is too small.");
    }

    // Write version, algorithm name, and source data size.
    int compressedBufferStartPosition = compressedBuffer.position();
    int sourceBufferStartPosition = sourceBuffer.position();
    try {
      byte[] algorithmNameBinary = getAlgorithmNameBinary();
      compressedBuffer.put((byte) 1);
      compressedBuffer.put((byte) algorithmNameBinary.length);
      compressedBuffer.put(algorithmNameBinary);
      compressedBuffer.putInt(sourceBuffer.remaining());

      // Apply compression and store the output in the compressed buffer.
      // Note: compressNative() uses less memory than buffer size and that's why it returns the actual compressed size.
      int sourceBufferLimit = sourceBuffer.limit();
      int compressedDataLength =
          compressNative(sourceBuffer, sourceBuffer.position(), sourceBuffer.remaining(), compressedBuffer,
              compressedBuffer.position(), compressedBuffer.capacity() - compressedBuffer.position());

      // compressNative() may or may not alter the position indexes, so set them just in case.
      sourceBuffer.position(sourceBufferLimit);  // Advance the sourceBuffer to limit to indicate entire buffer read.
      compressedBuffer.position(compressedBufferStartPosition + overheadSize + compressedDataLength);
      return (overheadSize + compressedDataLength);
    } catch (Exception ex) {
      // Exception thrown.  Restore the position of the buffers.
      sourceBuffer.position(sourceBufferStartPosition);
      compressedBuffer.position(compressedBufferStartPosition);
      throw ex;
    }
  }

  /**
   * Decompress the buffer specified in {@code compressedBuffer}.  Since compressedBuffer structure contains the
   * original data size, it reads only the portion required to decompress.
   * After calling, compressedBuffer position will be advanced to right after the compressed binary, and the
   * decompressedBuffer position will be advanced to the end of the decompressed binary.
   * If exception is thrown, the position index would not be updated.
   *
   * @param compressedBuffer The buffer that contains compressed data generated by the compress() method.
   * @param decompressedBuffer The buffer to hold the decompressed/original data.  The size should be at least the
   *                           size returned by getDecompressBufferSize().
   * @return The original data size which is same as number of bytes written to sourceData buffer.
   * @throws CompressionException Throws this exception if decompression has internal failure.
   */
  @Override
  public int decompress(ByteBuffer compressedBuffer, ByteBuffer decompressedBuffer) throws CompressionException {
    Utils.checkNotNullOrEmpty(compressedBuffer, "compressedBuffer cannot be null or empty.");
    Utils.checkNotNullOrEmpty(decompressedBuffer, "decompressedBuffer cannot be null or empty.");

    // Check compressed buffer size - must be bigger than header size without algorithm name.
    if (compressedBuffer.remaining() < VERSION_AND_NAME_LENGTH_AND_ORIGINAL_SIZE_SIZE) {
      throw new IllegalArgumentException(
          "compressedBuffer size of " + compressedBuffer.remaining() + " bytes is too small.");
    }

    // Check compressed buffer size - must be bigger than header size with algorithm name.
    int compressedBufferPosition = compressedBuffer.position();
    int algorithmNameLength = compressedBuffer.get(compressedBufferPosition + VERSION_SIZE);
    int headerSize = VERSION_AND_NAME_LENGTH_AND_ORIGINAL_SIZE_SIZE + algorithmNameLength;
    if (compressedBuffer.remaining() < headerSize) {
      throw new IllegalArgumentException("compressedBuffer is too small and does not contain original data size.");
    }

    // Get the original size from the compressed buffer.  It also verifies compressed buffer parameters.
    int originalSize = compressedBuffer.getInt(
        compressedBufferPosition + VERSION_SIZE + ALGORITHM_NAME_LENGTH_SIZE + algorithmNameLength);
    int decompressedBufferSize = decompressedBuffer.capacity() - decompressedBuffer.position();
    if (decompressedBufferSize < originalSize) {
      throw new IllegalArgumentException(
          "decompressedBuffer size " + decompressedBufferSize + " is too small to hold decompressed data size "
              + originalSize);
    }

    // Save the position.  Use position instead of mark just in case decompressNative() uses mark().
    int decompressedBufferPosition = decompressedBuffer.position();
    try {
      // Invoke native decompress method and advance the index positions.
      int compressedBufferLimit = compressedBuffer.limit();
      int newDecompressionBufferPosition = decompressedBuffer.position() + originalSize;
      decompressNative(compressedBuffer, compressedBuffer.position() + headerSize,
          compressedBuffer.remaining() - headerSize, decompressedBuffer, decompressedBuffer.position(), originalSize);
      compressedBuffer.position(compressedBufferLimit);
      decompressedBuffer.position(newDecompressionBufferPosition);
      return originalSize;
    } catch (Exception ex) {
      // Exception thrown, restore the positions.
      compressedBuffer.position(compressedBufferPosition);
      decompressedBuffer.position(decompressedBufferPosition);
      throw ex;
    }
  }
}
