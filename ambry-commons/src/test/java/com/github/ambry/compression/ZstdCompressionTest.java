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
  ZstdCompression compression = new ZstdCompression();

  @Test
  public void testGetAlgorithmName() {
    Assert.assertTrue(compression.getAlgorithmName().length() > 0);
    Assert.assertTrue(compression.getAlgorithmName().length() < BaseCompression.MAX_ALGORITHM_NAME_LENGTH);
  }

  @Test
  public void testCompressionLevel() {
    Assert.assertTrue(compression.getMinimumCompressionLevel() < 0);
    Assert.assertTrue(compression.getMaximumCompressionLevel() > 0);
    Assert.assertTrue(compression.getDefaultCompressionLevel() >= 0);
  }

  @Test
  public void testEstimateMaxCompressedDataSize() {
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(1) > 1);
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(100) > 100);
  }

  @Test
  public void testCompressAndDecompress_DefaultLevel() {
    compression.setCompressionLevel(compression.getDefaultCompressionLevel());
    LZ4CompressionTest.compressDataAndDecompressDataTest(compression, "Test default compression using default level.");
  }

  @Test
  public void testCompressAndDecompress_MinimumLevel() {
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    LZ4CompressionTest.compressDataAndDecompressDataTest(compression, "Test minimum compression using minimum level.");
  }

  @Test
  public void testCompressAndDecompress_MaximumLevel() {
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    LZ4CompressionTest.compressDataAndDecompressDataTest(compression, "Test maximum compression using maximum level.");
  }
}

