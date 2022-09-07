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

public class CompressionFactoryTest {

  @Test
  public void testGetDefault() {
    CompressionFactory factory = CompressionFactory.getDefault();
    Assert.assertNotNull(factory);
  }

  @Test
  public void testRegister() {
    CompressionFactory factory = new CompressionFactory();
    Compression lz4 = new LZ4Compression();
    factory.register(lz4);
    Compression compression = factory.getCompressorByName(lz4.getAlgorithmName());
    Assert.assertEquals(lz4, compression);
  }

  @Test
  public void testDefaultCompressor() {
    CompressionFactory factory = new CompressionFactory();
    Compression lz4 = new LZ4Compression();
    Assert.assertNull(factory.getDefaultCompressor());

    // Test: set default compressor fails due because it's not registerd.
    Exception ex = TestUtils.invokeAndGetException(() -> factory.setDefaultCompressor(lz4));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: register compression then set default.
    factory.register(lz4);
    factory.setDefaultCompressor(lz4);
    Compression compression = factory.getDefaultCompressor();
    Assert.assertEquals(lz4, compression);
  }

  @Test
  public void testGetDecompressor() {
    Compression lz4 = new LZ4Compression();
    Compression zstd = new ZstdCompression();

    CompressionFactory factory = new CompressionFactory();
    factory.register(lz4);
    factory.register(zstd);

    // Test: Invalid argument
    Exception ex = TestUtils.invokeAndGetException(() -> factory.getCompressionFromCompressedData(new byte[0]));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Compress the string using LZ4 and decompress using factory.
    String testMessage = "Ambry rocks.  Ambry is fast.  Ambry is the way to go.";
    byte[] testMessageBinary = testMessage.getBytes(StandardCharsets.UTF_8);
    Pair<Integer, byte[]> compressionResult = lz4.compress(testMessageBinary);
    byte[] decompressedData = decompressUsingFactory(factory, compressionResult);
    Assert.assertEquals(testMessage, new String(decompressedData, StandardCharsets.UTF_8));

    // Test: Compress the string using ZStd and decompress using factory.
    compressionResult = zstd.compress(testMessageBinary);
    decompressedData = decompressUsingFactory(factory, compressionResult);
    Assert.assertEquals(testMessage, new String(decompressedData, StandardCharsets.UTF_8));
  }

  private byte[] decompressUsingFactory(CompressionFactory factory, Pair<Integer, byte[]> compressionResult) {
    int bufferSize = compressionResult.getFirst();
    byte[] compressedBuffer = new byte[bufferSize];
    System.arraycopy(compressionResult.getSecond(), 0, compressedBuffer, 0, compressedBuffer.length);

    Compression decompressor = factory.getCompressionFromCompressedData(compressedBuffer);
    return decompressor.decompress(compressedBuffer);
  }
}