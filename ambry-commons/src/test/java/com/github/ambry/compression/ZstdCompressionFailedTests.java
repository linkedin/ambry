package com.github.ambry.compression;

import com.github.ambry.utils.TestUtils;
import com.github.luben.zstd.Zstd;
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
  public void testCompressFailed() {
    // When Zstd.compressByteArray() is called, return error code.
    PowerMockito.mockStatic(Zstd.class);
    PowerMockito.when(
        Zstd.compressByteArray(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(), Mockito.any(byte[].class),
            Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt())).thenReturn((long) -2);
    PowerMockito.when(Zstd.isError(Mockito.anyLong())).thenReturn(true);

    ZstdCompression zstd = new ZstdCompression();
    Exception ex =
        TestUtils.invokeAndGetException(() -> zstd.compress("ABC".getBytes(StandardCharsets.UTF_8), new byte[10], 0));
    Assert.assertTrue(ex instanceof CompressionException);
  }

  @Test
  public void testDecompressFailed() {
    // When Zstd.compressByteArray() is called, return error code.
    // Zstd.decompressByteArray(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize)
    PowerMockito.mockStatic(Zstd.class);
    PowerMockito.when(Zstd.decompressByteArray(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenReturn((long) -2);
    PowerMockito.when(Zstd.isError(Mockito.anyLong())).thenReturn(true);

    ZstdCompression zstd = new ZstdCompression();
    Exception ex = TestUtils.invokeAndGetException(() -> zstd.decompress(new byte[10], 0, new byte[10]));
    Assert.assertTrue(ex instanceof CompressionException);
  }

  @Test
  public void testDecompressWrongSize() {
    // When Zstd.compressByteArray() is called, return error code.
    // Zstd.decompressByteArray(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize)
    PowerMockito.mockStatic(Zstd.class);
    PowerMockito.when(Zstd.decompressByteArray(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
        Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenReturn(100L);

    ZstdCompression zstd = new ZstdCompression();
    Exception ex = TestUtils.invokeAndGetException(() -> zstd.decompress(new byte[10], 0, new byte[10]));
    Assert.assertTrue(ex instanceof CompressionException);
  }
}
