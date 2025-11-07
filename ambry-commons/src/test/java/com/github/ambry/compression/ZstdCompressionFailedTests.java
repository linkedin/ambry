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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ZstdCompressionFailedTests {
  @Test
  public void testCompressNativeFailed() {
    // Mock all Zstd static methods that might be called during compression failure
    try (MockedStatic<Zstd> zstdMock = Mockito.mockStatic(Zstd.class, invocation -> {
      String methodName = invocation.getMethod().getName();
      switch (methodName) {
        case "compressByteArray":
        case "compressDirectByteBuffer":
          return (long) -2;  // Return error code
        case "isError":
          return true;  // Indicate this is an error
        case "getErrorName":
          return "MockedErrorName";  // Return error name for exception message
        default:
          throw new UnsupportedOperationException("Unexpected method call: " + methodName);
      }
    })) {
      ZstdCompression zstd = new ZstdCompression();
      Exception ex = TestUtils.getException(() ->
          zstd.compressNative(ByteBuffer.wrap("ABC".getBytes(StandardCharsets.UTF_8)), 0, 3,
              ByteBuffer.wrap(new byte[10]), 0, 10));
      Assert.assertTrue(ex instanceof CompressionException);
    }
  }

  @Test
  public void testDecompressNativeFailed() {
    // Mock all Zstd static methods that might be called during decompression failure
    try (MockedStatic<Zstd> zstdMock = Mockito.mockStatic(Zstd.class, invocation -> {
      String methodName = invocation.getMethod().getName();
      switch (methodName) {
        case "decompressByteArray":
        case "decompressDirectByteBuffer":
          return (long) -2;  // Return error code
        case "isError":
          return true;  // Indicate this is an error
        case "getErrorName":
          return "MockedErrorName";  // Return error name for exception message
        default:
          throw new UnsupportedOperationException("Unexpected method call: " + methodName);
      }
    })) {
      ZstdCompression zstd = new ZstdCompression();
      Exception ex = TestUtils.getException(() ->
          zstd.decompressNative(ByteBuffer.wrap(new byte[10]), 0, 10, ByteBuffer.wrap(new byte[10]), 0, 10));
      Assert.assertTrue(ex instanceof CompressionException);
    }
  }
}
