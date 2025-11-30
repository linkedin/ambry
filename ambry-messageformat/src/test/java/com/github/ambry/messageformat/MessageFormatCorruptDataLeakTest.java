/*
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.messageformat;

import com.github.ambry.commons.DelegateByteBuf;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests ByteBuf leak detection and prevention in blob deserialization error paths. When
 * {@code MessageFormatRecord.Blob_Format_V1/V2/V3.deserializeBlobRecord} allocates a ByteBuf slice
 * via {@code Utils.readNettyByteBufFromCrcInputStream} but fails, make sure the memory is correctly released.
 */
@RunWith(Parameterized.class)
public class MessageFormatCorruptDataLeakTest {
  private final short blobVersion;
  /**
   * NettyByteBufDataInputStream that captures ByteBuf slices created during deserialization.
   * Overrides {@code getBuffer()} to return a {@link SliceCapturingByteBuf} wrapper that
   * intercepts {@code slice(int, int)} calls.
   */
  private static class CapturingInputStream extends NettyByteBufDataInputStream {
    private final SliceCapturingByteBuf wrapper;

    public CapturingInputStream(ByteBuf buffer) {
      super(buffer);
      this.wrapper = new SliceCapturingByteBuf(buffer);
    }

    @Override
    public ByteBuf getBuffer() {
      return wrapper;
    }

    public List<ByteBuf> getCapturedSlices() {
      return wrapper.getCapturedSlices();
    }
  }

  /**
   * ByteBuf wrapper that captures slice creation for leak detection testing.
   * Extends {@link DelegateByteBuf} and overrides only {@code slice(int, int)} to intercept
   * and record all slice creations. All other ByteBuf methods delegate to the underlying buffer.
   */
  private static class SliceCapturingByteBuf extends DelegateByteBuf {
    private final List<ByteBuf> capturedSlices = new ArrayList<>();

    public SliceCapturingByteBuf(ByteBuf delegate) {
      super(delegate);
    }

    public List<ByteBuf> getCapturedSlices() {
      return Collections.unmodifiableList(capturedSlices);
    }

    @Override
    public ByteBuf retainedSlice(int index, int length) {
      ByteBuf slice = super.slice(index, length);
      capturedSlices.add(slice);
      return slice;
    }
  }

  /**
   * Running for all blob format versions (V1, V2, V3).
   * @return a List with all blob versions.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {MessageFormatRecord.Blob_Version_V1},
        {MessageFormatRecord.Blob_Version_V2},
        {MessageFormatRecord.Blob_Version_V3}
    });
  }

  /**
   * Constructor with parameter to be set.
   * @param blobVersion The blob version to test.
   */
  public MessageFormatCorruptDataLeakTest(short blobVersion) {
    this.blobVersion = blobVersion;
  }

  /**
   * Creates a ByteBuf with the blob format for the given version.
   * @param blobVersion The blob version (V1, V2, or V3).
   * @param blobSize The size of the blob content.
   * @return A ByteBuf containing the serialized blob record without CRC.
   */
  private ByteBuf createBlobRecordBuffer(short blobVersion, int blobSize) {
    int bufferSize;
    if (blobVersion == MessageFormatRecord.Blob_Version_V1) {
      bufferSize = 2 + 8 + blobSize; // version + size + content
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2) {
      bufferSize = 2 + 2 + 8 + blobSize; // version + blobType + size + content
    } else { // V3
      bufferSize = 2 + 2 + 1 + 8 + blobSize; // version + blobType + isCompressed + size + content
    }

    ByteBuf inputBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSize);
    byte[] blobContent = new byte[blobSize];

    inputBuf.writeShort(blobVersion);
    if (blobVersion >= MessageFormatRecord.Blob_Version_V2) {
      inputBuf.writeShort((short) BlobType.DataBlob.ordinal());
    }
    if (blobVersion >= MessageFormatRecord.Blob_Version_V3) {
      inputBuf.writeByte(0);
    }
    inputBuf.writeLong(blobSize);
    inputBuf.writeBytes(blobContent);

    return inputBuf;
  }

  /**
   * Test corrupt blob deserialization with CRC mismatch - verifies ByteBuf cleanup on error path.
   * Parameterized test for all blob format versions (V1, V2, V3).
   */
  @Test
  public void testDeserializeBlobWithCorruptCrc() throws Exception {
    int blobSize = 1024;
    ByteBuf inputBuf = createBlobRecordBuffer(blobVersion, blobSize);

    // Corrupt the CRC to trigger validation failure
    CRC32 crc = new CRC32();
    crc.update(inputBuf.nioBuffer(0, inputBuf.writerIndex()));
    inputBuf.writeLong(crc.getValue() + 1);

    CapturingInputStream capturingStream = new CapturingInputStream(inputBuf);

    try {
      MessageFormatRecord.deserializeBlob(capturingStream);
      fail("Should have thrown MessageFormatException due to corrupt CRC");
    } catch (MessageFormatException e) {
      assertEquals(MessageFormatErrorCodes.DataCorrupt, e.getErrorCode());
    }

    List<ByteBuf> capturedSlices = capturingStream.getCapturedSlices();
    assertEquals("Expected exactly one slice to be created", 1, capturedSlices.size());
    ByteBuf slice = capturedSlices.get(0);
    assertEquals("ByteBuf slice must be released after CRC validation failure", 0, slice.refCnt());
  }

  /**
   * Test valid blob deserialization - verifies no leak on success path (control test).
   * This test ensures the leak detection mechanism doesn't produce false positives.
   * Parameterized test for all blob format versions (V1, V2, V3).
   */
  @Test
  public void testDeserializeBlobWithValidCrc() throws Exception {
    int blobSize = 512;
    ByteBuf inputBuf = createBlobRecordBuffer(blobVersion, blobSize);

    CRC32 crc = new CRC32();
    crc.update(inputBuf.nioBuffer(0, inputBuf.writerIndex()));
    inputBuf.writeLong(crc.getValue());

    CapturingInputStream capturingStream = new CapturingInputStream(inputBuf);

    BlobData blobData = MessageFormatRecord.deserializeBlob(capturingStream);

    assertEquals("BlobData should contain " + blobSize + " bytes", blobSize, blobData.content().readableBytes());
    List<ByteBuf> capturedSlices = capturingStream.getCapturedSlices();
    assertEquals("Expected exactly one slice to be created", 1, capturedSlices.size());
    ByteBuf slice = capturedSlices.get(0);
    assertEquals("Slice should have refCnt == 1 before release", 1, slice.refCnt());
  }
}
