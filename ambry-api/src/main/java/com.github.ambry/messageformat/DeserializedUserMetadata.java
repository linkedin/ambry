package com.github.ambry.messageformat;

import java.nio.ByteBuffer;


public class DeserializedUserMetadata {
  private final short version;
  private final ByteBuffer userMetadata;

  public DeserializedUserMetadata(short version, ByteBuffer userMetadata) {
    this.version = version;
    this.userMetadata = userMetadata;
  }

  public short getVersion() {
    return version;
  }

  public ByteBuffer getUserMetadata() {
    return userMetadata;
  }
}
