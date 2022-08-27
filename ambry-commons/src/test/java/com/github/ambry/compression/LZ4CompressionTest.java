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
import org.junit.Assert;
import org.junit.Test;

public class LZ4CompressionTest {
  LZ4Compression compression = new LZ4Compression();

  @Test
  public void testGetAlgorithmName() {
    Assert.assertTrue(compression.getAlgorithmName().length() > 0);
    Assert.assertTrue(compression.getAlgorithmName().length() < BaseCompression.MAX_ALGORITHM_NAME_LENGTH);
  }

  @Test
  public void testCompressionLevel() {
    Assert.assertEquals(0, compression.getMinimumCompressionLevel());
    Assert.assertTrue(compression.getMaximumCompressionLevel() > 0);
    Assert.assertTrue(compression.getDefaultCompressionLevel() >= 0);
  }

  @Test
  public void testEstimateMaxCompressedDataSize() {
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(1) > 1);
    Assert.assertTrue(compression.estimateMaxCompressedDataSize(100) > 100);
  }

  @Test
  public void testCompressAndDecompress_MinimumLevel() {
    compression.setCompressionLevel(compression.getMinimumCompressionLevel());
    runCompressionAndDecompressionTest(compression, "Test my minimum compression message using minimum level.");
  }

  @Test
  public void testCompressAndDecompress_MaximumLevel() {
    compression.setCompressionLevel(compression.getMaximumCompressionLevel());
    runCompressionAndDecompressionTest(compression, "Test my maximum compression message using maximum level.");
  }

  public static void runCompressionAndDecompressionTest(BaseCompression compression, String testMessage) {
    // Apply compression to testMessage.
    byte[] sourceBuffer = testMessage.getBytes(StandardCharsets.UTF_8);
    byte[] oversizeCompressedBuffer = new byte[compression.estimateMaxCompressedDataSize(sourceBuffer.length)];
    int usage = compression.compress(sourceBuffer, oversizeCompressedBuffer, 0);
    Assert.assertTrue(usage > 0);

    // Apply decompression.
    byte[] compressedBuffer = new byte[usage];
    System.arraycopy(oversizeCompressedBuffer, 0, compressedBuffer, 0, usage);
    byte[] decompressedBuffer = new byte[sourceBuffer.length];
    compression.decompress(compressedBuffer, 0, decompressedBuffer);
    String decompressedMessage = new String(decompressedBuffer, StandardCharsets.UTF_8);
    Assert.assertEquals(testMessage, decompressedMessage);
  }
}