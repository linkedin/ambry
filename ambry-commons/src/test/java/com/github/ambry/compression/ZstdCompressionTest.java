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

import org.junit.Assert;
import org.junit.Test;

public class ZstdCompressionTest {
  @Test
  public void testGetAlgorithmName() {
    ZstdCompression compression = new ZstdCompression();
    Assert.assertTrue(compression.getAlgorithmName().length() > 0);
    Assert.assertTrue(compression.getAlgorithmName().length() < BaseCompression.MAX_ALGORITHM_NAME_LENGTH);
  }

  @Test
  public void testCompressionLevel() {
    ZstdCompression compression = new ZstdCompression();
    Assert.assertTrue(compression.getMinimumCompressionLevel() < 0);
    Assert.assertTrue(compression.getMaximumCompressionLevel() > 0);
    Assert.assertTrue(compression.getDefaultCompressionLevel() >= 0);
  }

  @Test
  public void testEstimateMaxCompressedDataSize() {
    ZstdCompression compression = new ZstdCompression();
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(1) > 1);
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(100) > 100);
  }

  @Test
  public void testCompressAndDecompress_DefaultLevel() throws CompressionException {
    ZstdCompression compression = new ZstdCompression();
    compression.setCompressionLevel(compression.getDefaultCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    LZ4CompressionTest.compressAndDecompressNativeTest(compression, "Test default compression using default level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    LZ4CompressionTest.compressAndDecompressNativeTest(compression, "Test default compression using default level.", 1, 2);
  }

  @Test
  public void testCompressAndDecompress_MinimumLevel() throws CompressionException {
    ZstdCompression compression = new ZstdCompression();
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    LZ4CompressionTest.compressAndDecompressTest(compression, "Test minimum compression using minimum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    LZ4CompressionTest.compressAndDecompressTest(compression, "Test minimum compression using minimum level.", 2, 2);
  }

  @Test
  public void testCompressAndDecompressNative_MinimumLevel() throws CompressionException {
    ZstdCompression compression = new ZstdCompression();
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    LZ4CompressionTest.compressAndDecompressNativeTest(compression, "Test minimum compression using minimum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    LZ4CompressionTest.compressAndDecompressNativeTest(compression, "Test minimum compression using minimum level.", 2, 2);
  }

  @Test
  public void testCompressAndDecompress_MaximumLevel() throws CompressionException {
    ZstdCompression compression = new ZstdCompression();
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    LZ4CompressionTest.compressAndDecompressTest(compression, "Test maximum compression using maximum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    LZ4CompressionTest.compressAndDecompressTest(compression, "Test maximum compression using maximum level.", 3, 0);
  }

  @Test
  public void testCompressAndDecompressNative_MaximumLevel() throws CompressionException {
    ZstdCompression compression = new ZstdCompression();
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    // Test dedicated buffer (without extra bytes on left or right of buffer).
    LZ4CompressionTest.compressAndDecompressNativeTest(compression, "Test maximum compression using maximum level.", 0, 0);

    // Test mid-buffer (with extra bytes on left and right of buffer.)
    LZ4CompressionTest.compressAndDecompressNativeTest(compression, "Test maximum compression using maximum level.", 3, 0);
  }
}
