package com.github.ambry.cloud;

import com.github.ambry.store.FindToken;
import java.nio.ByteBuffer;


/**
 * FindToken implementation used by the {@link CloudBlobStore}.
 */
public class CloudFindToken implements FindToken {

  // TODO: add version
  private final long latestUploadTime;
  private final long bytesRead;

  /** Constructor for start token */
  public CloudFindToken() {
    this(0, 0);
  }

  /** Constructor for in-progress token */
  public CloudFindToken(long latestUploadTime, long bytesRead) {
    this.latestUploadTime = latestUploadTime;
    this.bytesRead = bytesRead;
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(2 * Long.BYTES);
    byteBuffer.putLong(latestUploadTime);
    byteBuffer.putLong(bytesRead);
    return byteBuffer.array();
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  public long getLatestUploadTime() {
    return latestUploadTime;
  }
}
