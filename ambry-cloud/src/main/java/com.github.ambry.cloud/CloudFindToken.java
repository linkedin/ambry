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
import java.util.List;
import java.util.Objects;


/**
 * FindToken implementation used by the {@link CloudBlobStore}.
 */
public class CloudFindToken implements FindToken {

  static final short VERSION_0 = 0;
  static final short CURRENT_VERSION = VERSION_0;
  private final short version;
  private final FindTokenType type;
  private final long lastUpdateTime;
  private final String latestBlobId;
  private final long bytesRead;

  /** Constructor for start token */
  public CloudFindToken() {
    this(0, null, 0);
  }

  /** Constructor for in-progress token */
  public CloudFindToken(long lastUpdateTime, String latestBlobId, long bytesRead) {
    this.version = CURRENT_VERSION;
    this.type = FindTokenType.CloudBased;
    this.lastUpdateTime = lastUpdateTime;
    this.latestBlobId = latestBlobId;
    this.bytesRead = bytesRead;
  }

  /** Constructor for reading token that can have older version*/
  public CloudFindToken(short version, long lastUpdateTime, String latestBlobId, long bytesRead) {
    this.version = version;
    this.type = FindTokenType.CloudBased;
    this.latestBlobId = latestBlobId;
    this.lastUpdateTime = lastUpdateTime;
    this.bytesRead = bytesRead;
  }

  /**
   * Utility to construct a new CloudFindToken from a previous instance and the results of a findEntriesSince query.
   * @param prevToken the previous CloudFindToken.
   * @param queryResults the results of a findEntriesSince query.
   * @return the updated token.
   */
  public static CloudFindToken getUpdatedToken(CloudFindToken prevToken, List<CloudBlobMetadata> queryResults) {
    if (queryResults.isEmpty()) {
      return prevToken;
    } else {
      CloudBlobMetadata lastResult = queryResults.get(queryResults.size() - 1);
      long bytesReadThisQuery = queryResults.stream().mapToLong(CloudBlobMetadata::getSize).sum();
      return new CloudFindToken(lastResult.getLastUpdateTime(), lastResult.getId(),
          prevToken.getBytesRead() + bytesReadThisQuery);
    }
  }

  @Override
  public byte[] toBytes() {
    byte[] buf = null;
    switch (version) {
      case VERSION_0:
        int size = 3 * Short.BYTES + 2 * Long.BYTES;
        if (latestBlobId != null) {
          size += latestBlobId.length();
        }
        buf = new byte[size];
        ByteBuffer bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(version);
        // add type
        bufWrap.putShort((short) type.ordinal());
        // add latestUploadTime
        bufWrap.putLong(lastUpdateTime);
        // add bytesRead
        bufWrap.putLong(bytesRead);
        if (latestBlobId != null) {
          bufWrap.putShort((short) latestBlobId.length());
          bufWrap.put(latestBlobId.getBytes());
        } else {
          bufWrap.putShort((short) 0);
        }
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
        long lastUpdateTime = stream.readLong();
        long bytesRead = stream.readLong();
        short latestBlobIdLength = stream.readShort();
        String latestBlobId = null;
        if (latestBlobIdLength != 0) {
          byte[] latestBlobIdbytes = new byte[latestBlobIdLength];
          stream.read(latestBlobIdbytes, 0, (int) latestBlobIdLength);
          latestBlobId = new String(latestBlobIdbytes);
        }
        cloudFindToken = new CloudFindToken(version, lastUpdateTime, latestBlobId, bytesRead);
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

  public String getLatestBlobId() {
    return latestBlobId;
  }

  public long getLastUpdateTime() {
    return lastUpdateTime;
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
    return version == that.version && lastUpdateTime == that.lastUpdateTime && bytesRead == that.bytesRead
        && Objects.equals(latestBlobId, that.latestBlobId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, lastUpdateTime, latestBlobId, bytesRead);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version: ").append(version);
    sb.append(" lastUpdateTime: ").append(lastUpdateTime);
    sb.append(" latestBlobId: ").append(latestBlobId);
    sb.append(" bytesRead: ").append(bytesRead);
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
