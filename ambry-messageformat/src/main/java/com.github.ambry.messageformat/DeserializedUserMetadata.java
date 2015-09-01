package com.github.ambry.messageformat;

import java.nio.ByteBuffer;


class DeserializedUserMetadata {
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
