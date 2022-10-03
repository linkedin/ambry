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
import com.github.ambry.utils.TestUtils;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class CompressionMapTest {

  @Test
  public void testOf() {
    CompressionMap map = CompressionMap.of(new LZ4Compression(), new ZstdCompression());
    Assert.assertNotNull(map);
    Assert.assertEquals(2, map.size());
  }

  @Test
  public void testAdd() {
    CompressionMap map = new CompressionMap();

    // Test invalid parameter case.
    Exception ex = TestUtils.getException(() -> map.add(null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Test normal case.
    Compression lz4 = new LZ4Compression();
    map.add(lz4);
    Compression compression = map.get(lz4.getAlgorithmName());
    Assert.assertEquals(lz4, compression);
  }

  @Test
  public void testGetAlgorithmName() throws CompressionException {
    Compression lz4 = new LZ4Compression();
    Compression zstd = new ZstdCompression();
    CompressionMap map = CompressionMap.of(lz4, zstd);

    // Test: Invalid argument
    Exception ex = TestUtils.getException(() -> map.getByName(null));
    Assert.assertTrue(ex instanceof NullPointerException);

    Assert.assertEquals(lz4, map.getByName(lz4.getAlgorithmName()));
    Assert.assertEquals(zstd, map.getByName(zstd.getAlgorithmName()));
  }

  @Test
  public void testGetDecompressor() throws CompressionException {
    Compression lz4 = new LZ4Compression();
    Compression zstd = new ZstdCompression();

    CompressionMap map = CompressionMap.of(lz4, zstd);

    // Test: Invalid argument
    Exception ex = TestUtils.getException(() -> map.getByName(null));
    Assert.assertTrue(ex instanceof NullPointerException);

    // Test: Compress the string using LZ4 and decompress using factory.
    String testMessage = "Ambry rocks.  Ambry again.  Ambry again.";
    byte[] testMessageBinary = testMessage.getBytes(StandardCharsets.UTF_8);
    Pair<Integer, byte[]> compressionResult = lz4.compress(testMessageBinary);
    byte[] decompressedData = decompressUsingFactory(map, compressionResult);
    Assert.assertEquals(testMessage, new String(decompressedData, StandardCharsets.UTF_8));

    // Test: Compress the string using ZStd and decompress using factory.
    compressionResult = zstd.compress(testMessageBinary);
    decompressedData = decompressUsingFactory(map, compressionResult);
    Assert.assertEquals(testMessage, new String(decompressedData, StandardCharsets.UTF_8));
  }

  private byte[] decompressUsingFactory(CompressionMap factory, Pair<Integer, byte[]> compressionResult)
      throws CompressionException {
    int bufferSize = compressionResult.getFirst();
    byte[] compressedBuffer = new byte[bufferSize];
    System.arraycopy(compressionResult.getSecond(), 0, compressedBuffer, 0, compressedBuffer.length);

    String algorithmName = factory.getAlgorithmName(compressedBuffer, 0, compressedBuffer.length);
    Compression decompressor = factory.getByName(algorithmName);
    return decompressor.decompress(compressedBuffer);
  }
}
