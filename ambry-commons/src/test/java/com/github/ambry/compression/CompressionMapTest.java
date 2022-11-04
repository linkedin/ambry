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

import com.github.ambry.utils.TestUtils;
import java.nio.ByteBuffer;
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
  public void testGetAlgorithmName() {
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
    ByteBuffer testMessageBuffer = ByteBuffer.wrap(testMessage.getBytes(StandardCharsets.UTF_8));
    ByteBuffer compressedBuffer = lz4.compress(testMessageBuffer, false);
    byte[] decompressedBuffer = decompressUsingFactory(map, compressedBuffer);
    Assert.assertEquals(testMessage, new String(decompressedBuffer, StandardCharsets.UTF_8));

    // Test: Compress the string using ZStd and decompress using factory.
    testMessageBuffer.position(0);
    compressedBuffer = zstd.compress(testMessageBuffer, false);
    decompressedBuffer = decompressUsingFactory(map, compressedBuffer);
    Assert.assertEquals(testMessage, new String(decompressedBuffer, StandardCharsets.UTF_8));
  }

  private byte[] decompressUsingFactory(CompressionMap factory, ByteBuffer compressedBuffer)
      throws CompressionException {
    String algorithmName = factory.getAlgorithmName(compressedBuffer);
    Compression decompressor = factory.getByName(algorithmName);
    ByteBuffer decompressedBuffer = decompressor.decompress(compressedBuffer, false);

    byte[] sourceBinary = new byte[decompressedBuffer.remaining()];
    decompressedBuffer.get(sourceBinary);
    return sourceBinary;
  }
}
