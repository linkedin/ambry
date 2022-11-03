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
import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Zstd.class)
public class ZstdCompressionFailedTests {
  @Test
  public void testCompressNativeFailed() {
    // When Zstd.compressByteArray() is called, return error code.
    PowerMockito.mockStatic(Zstd.class);
    PowerMockito.when(Zstd.compressByteArray(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt())).thenReturn((long) -2);
    PowerMockito.when(Zstd.isError(Mockito.anyLong())).thenReturn(true);

    ZstdCompression zstd = new ZstdCompression();
    Exception ex = TestUtils.getException(() ->
        zstd.compressNative(ByteBuffer.wrap("ABC".getBytes(StandardCharsets.UTF_8)), 0, 3,
            ByteBuffer.wrap(new byte[10]), 0, 10));
    Assert.assertTrue(ex instanceof CompressionException);
  }

  @Test
  public void testDecompressNativeFailed() {
    // When Zstd.compressByteArray() is called, return error code.
    // Zstd.decompressByteArray(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize)
    PowerMockito.mockStatic(Zstd.class);
    PowerMockito.when(Zstd.decompressByteArray(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenReturn((long) -2);
    PowerMockito.when(Zstd.isError(Mockito.anyLong())).thenReturn(true);

    ZstdCompression zstd = new ZstdCompression();
    Exception ex = TestUtils.getException(() ->
        zstd.decompressNative(ByteBuffer.wrap(new byte[10]), 0, 10, ByteBuffer.wrap(new byte[10]), 0, 10));
    Assert.assertTrue(ex instanceof CompressionException);
  }
}
