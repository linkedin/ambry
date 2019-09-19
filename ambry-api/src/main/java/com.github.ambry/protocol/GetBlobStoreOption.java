package com.github.ambry.protocol;

import com.github.ambry.router.ByteRange;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class GetBlobStoreOption {
  private ByteRange byteRange;
  private static final short GetBlobStoreOption_V1 = 1;

  private static final int Version_Size_In_Bytes = Short.BYTES;
  private static final int ByteRange_Size_In_Bytes = Long.BYTES * 2;

  public GetBlobStoreOption(DataInputStream stream) throws IOException {
    short version = stream.readShort();
    if (version != GetBlobStoreOption_V1) {
      throw new IllegalArgumentException("Version " + version + " not supported in GetBlobStoreOption");
    }
    long startOffset = stream.readLong();
    long endOffset = stream.readLong();
    byteRange = ByteRange.fromOffsetRange(startOffset, endOffset);
  }

  public void writeTo(ByteBuffer buffer) {
    buffer.putShort(GetBlobStoreOption_V1);
    buffer.putLong(byteRange.getStartOffset());
    buffer.putLong(byteRange.getEndOffset());
  }

  public long sizeInBytes() {
    return Version_Size_In_Bytes + ByteRange_Size_In_Bytes;
  }

  public GetBlobStoreOption(ByteRange byteRange) {
    this.byteRange = byteRange;
  }

  public ByteRange getByteRange() {
    return byteRange;
  }

  public String toString() {
    return "ByteRange:" + byteRange.getStartOffset() + "->" + byteRange.getEndOffset();
  }
}
