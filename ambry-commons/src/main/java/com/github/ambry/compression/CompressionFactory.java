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
import java.util.HashMap;
import java.util.Map;

/**
 * CompressionFactory is a compression algorithm registration keyed by the algorithm name.
 * The factory contains a static shared instance with all supported algorithms pre-registered.
 *
 * Factory APIs include:
 * - Register a new compression algorithm.  The shared instance has pre-registered a set of compression algorithms.
 * - Get a registered compression by name.
 * - Get default compressor.
 * - Set default compressor.
 * - Get decompressor by compressed buffer generated by compress().
 */
public class CompressionFactory {

  // The static instance of compression factory.
  private static volatile CompressionFactory defaultFactory;

  /**
   * Get the shared instance of compression factory.
   * The shared instance contains a pre-registered set of supported compression algorithms.
   * @return The shared compression factory instance.
   */
  public static CompressionFactory getDefault() {
    if (defaultFactory != null) return defaultFactory;

    synchronized(CompressionFactory.class) {
      if (defaultFactory == null) {
        defaultFactory = new CompressionFactory();

        // ****** WARNING ********
        // You may register/add more compression services, but DO NOT remove any existing compression service.
        // Decompression relies on this map to locate the decompression algorithm using the algorithm name.
        defaultFactory.register(new LZ4Compression());
        defaultFactory.register(new ZstdCompression());
      }
    }

    return defaultFactory;
  }

  // Map from algorithm name to instance of compression service.
  private final Map<String, Compression> registeredCompressionMap = new HashMap<>();

  // The default compressor to use to compress new data.
  private Compression defaultCompressor;

  /**
   * Register a compression service.  Every compression service has a unique name.
   * Decompression will use the registered compression services to find the decompression algorithm.
   *
   * @param newCompressionService The compression service to register.
   */
  public void register(Compression newCompressionService) {
    if (newCompressionService == null) {
      throw new IllegalArgumentException("newCompressionService");
    }
    registeredCompressionMap.put(newCompressionService.getAlgorithmName(), newCompressionService);
  }

  /**
   * To get a compressor by algorithm name.
   * The application config could call this method to set the default compressor algorithm.
   *      CompressionFactory factory = CompressionFactory.getDefault();
   *      Compression compressor = factory.getCompressorByName("LZ4");
   *      factory.setDefaultCompressor(compressor);
   *
   * @param algorithmName Name of the algorithm to get.
   * @return Instance of the compression algorithm; null if not found.
   */
  public Compression getCompressorByName(String algorithmName) {
    return registeredCompressionMap.get(algorithmName);
  }

  /**
   * Set the default compressor to use when compressing data.  The new compressor can be null to clear default.
   * If a new compressor is specified, it must be registered.
   * @param newCompressor The new compressor to use.
   */
  public void setDefaultCompressor(Compression newCompressor) {
    if (newCompressor != null && !registeredCompressionMap.containsKey(newCompressor.getAlgorithmName())) {
      throw new IllegalArgumentException("newCompressor is not registered.");
    }
    defaultCompressor = newCompressor;
  }

  /**
   * Get the default compressor to use when compressing data.
   * It returns the compressor set by setDefaultCompressor().
   * @return The default compressor.  Return null if not set.
   */
  public Compression getDefaultCompressor() {
    return defaultCompressor;
  }

  /**
   * Given the compressed buffer generated by compressor's compress() method,
   * find the decompressor based on the algorithm name stored in compressedBuffer.
   *
   * @param compressedBuffer The compressed buffer generated by the compress() method.
   * @return The decompressor that can decompress this compressed buffer.  Null if no decompressor found.
   */
  public Compression getCompressionFromCompressedData(byte[] compressedBuffer) {
    // Compressed buffer contains at least version (1 byte), algorithm name size (1 byte) and original size (4 bytes).
    if (compressedBuffer == null || compressedBuffer.length < 6) {
      throw new IllegalArgumentException("compressedBuffer must be at least 6 bytes long.");
    }

    int algorithmNameLength = compressedBuffer[1];
    String algorithmName = new String(compressedBuffer, 2, algorithmNameLength, StandardCharsets.UTF_8);
    return registeredCompressionMap.get(algorithmName);
  }
}