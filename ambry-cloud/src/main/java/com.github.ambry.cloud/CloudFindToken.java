/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;


/**
 * FindToken implementation used by the {@link CloudBlobStore}.
 */
public class CloudFindToken implements FindToken {

  static final short VERSION_0 = 0;
  static final short CURRENT_VERSION = VERSION_0;
  private final short version;
  private final FindTokenType type;
  private final AzureFindToken azureFindToken;
  private final long bytesRead;

  /** Constructor for start token */
  public CloudFindToken() {
    this(VERSION_0, 0, new AzureFindToken());
  }

  /** Constructor for in-progress token */
  public CloudFindToken(long bytesRead, AzureFindToken azureFindToken) {
    this.version = CURRENT_VERSION;
    this.type = FindTokenType.CloudBased;
    this.bytesRead = bytesRead;
    this.azureFindToken = azureFindToken;
  }

  /** Constructor for reading token that can have older version*/
  public CloudFindToken(short version, long bytesRead, AzureFindToken azureFindToken) {
    this.version = version;
    this.type = FindTokenType.CloudBased;
    this.bytesRead = bytesRead;
    this.azureFindToken = azureFindToken;
  }

  /**
   * Utility to construct a new CloudFindToken from a previous instance and new token returned from findEntriesSince query.
   * @param prevToken previous {@link CloudFindToken}.
   * @param azureFindToken new {@link AzureFindToken}
   * @param newBytesRead bytes read in the findEntriesSince query.
   * @return the updated token.
   */
  public static CloudFindToken getUpdatedToken(CloudFindToken prevToken, AzureFindToken azureFindToken,
      long newBytesRead) {
    if (azureFindToken.equals(prevToken.azureFindToken)) {
      return prevToken;
    }

    return new CloudFindToken(prevToken.getBytesRead() + newBytesRead, azureFindToken);
  }

  @Override
  public byte[] toBytes() {
    byte[] buf = null;
    switch (version) {
      case VERSION_0:
        int size = 2 * Short.BYTES + Long.BYTES + azureFindToken.size();
        buf = new byte[size];
        ByteBuffer bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(version);
        // add type
        bufWrap.putShort((short) type.ordinal());
        // add bytesRead
        bufWrap.putLong(bytesRead);
        // add lastUpdateTimeReadBlobIds
        bufWrap.put(azureFindToken.toBytes());
        break;
      default:
        throw new IllegalStateException("Unknown version: " + version);
    }
    return buf;
  }

  /**
   * Utility to construct a previously serialized {@code CloudFindToken} from input stream.
   * @param inputStream {@code DataInputStream} from which to read the token.
   * @return deserialized {@code CloudFindToken} object.
   * @throws IOException
   */
  static CloudFindToken fromBytes(DataInputStream inputStream) throws IOException {
    CloudFindToken cloudFindToken = null;
    DataInputStream stream = new DataInputStream(inputStream);
    short version = stream.readShort();
    switch (version) {
      case VERSION_0:
        FindTokenType type = FindTokenType.values()[stream.readShort()];
        long bytesRead = stream.readLong();
        AzureFindToken azureFindToken = AzureFindToken.fromBytes(stream);
        cloudFindToken = new CloudFindToken(version, bytesRead, azureFindToken);
        break;
      default:
        throw new IllegalStateException("Unknown version: " + version);
    }
    return cloudFindToken;
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  public AzureFindToken getAzureFindToken() {
    return azureFindToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CloudFindToken that = (CloudFindToken) o;
    return version == that.version && bytesRead == that.bytesRead && azureFindToken.equals(
        ((CloudFindToken) o).getAzureFindToken());
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, azureFindToken, bytesRead);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version: ").append(version);
    sb.append(" bytesRead: ").append(bytesRead);
    sb.append(" _azureFindToken: ").append(azureFindToken.toString());
    return sb.toString();
  }

  @Override
  public FindTokenType getType() {
    return type;
  }

  @Override
  public short getVersion() {
    return version;
  }
}
