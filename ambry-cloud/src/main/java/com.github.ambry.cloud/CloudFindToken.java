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
import com.github.ambry.utils.PeekableInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


/**
 * FindToken implementation used by the {@link CloudBlobStore}.
 */
public class CloudFindToken implements FindToken {

  static final short VERSION_0 = 0;
  static final short VERSION_3 = 3;
  static final short CURRENT_VERSION = VERSION_3;
  private final short version;
  private final FindTokenType type;
  private final long latestUploadTime;
  private final String latestBlobId;
  private final long bytesRead;

  /** Constructor for start token */
  public CloudFindToken() {
    this(0, null, 0);
  }

  /** Constructor for in-progress token */
  public CloudFindToken(long latestUploadTime, String latestBlobId, long bytesRead) {
    this.version = CURRENT_VERSION;
    this.type = FindTokenType.CloudBased;
    this.latestUploadTime = latestUploadTime;
    this.latestBlobId = latestBlobId;
    this.bytesRead = bytesRead;
  }

  /** Constructor for reading token that can have older version*/
  public CloudFindToken(short version, long latestUploadTime, String latestBlobId, long bytesRead) {
    this.version = version;
    this.type = FindTokenType.CloudBased;
    this.latestBlobId = latestBlobId;
    this.latestUploadTime = latestUploadTime;
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
      return new CloudFindToken(lastResult.getUploadTime(), lastResult.getId(), prevToken.getBytesRead() + bytesReadThisQuery);
    }
  }

  @Override
  public byte[] toBytes() {
    byte[] buf = null;
    switch (version) {
      case VERSION_0:
        int size = 2 * Short.BYTES + 2 * Long.BYTES;
        if(latestBlobId != null) {
          size += latestBlobId.length();
        }
        buf = new byte[size];
        ByteBuffer bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(version);
        // add latestUploadTime
        bufWrap.putLong(latestUploadTime);
        // add bytesRead
        bufWrap.putLong(bytesRead);
        if (latestBlobId != null) {
          bufWrap.putShort((short) latestBlobId.length());
          bufWrap.put(latestBlobId.getBytes());
        } else {
          bufWrap.putShort((short) 0);
        }
        break;
      case VERSION_3:
        size = 3 * Short.BYTES + 2 * Long.BYTES;
        if(latestBlobId != null) {
          size += latestBlobId.length();
        }
        buf = new byte[size];
        bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(version);
        // add type
        bufWrap.putShort((short) type.ordinal());
        // add latestUploadTime
        bufWrap.putLong(latestUploadTime);
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
   * @param inputStream {@code PeekableInputStream} from which to read the token.
   * @return deserialized {@code CloudFindToken} object.
   * @throws IOException
   */
  static CloudFindToken fromBytes(PeekableInputStream inputStream) throws IOException {
    CloudFindToken cloudFindToken = null;
    DataInputStream stream = new DataInputStream(inputStream);
    short version = stream.readShort();
    if(version < VERSION_3) {
      throw new IllegalArgumentException("Unrecognized version in CloudFindToken: " + version);
    }
    switch(version) {
      case VERSION_0:
        long latestUploadTime = stream.readLong();
        long bytesRead = stream.readLong();
        short latestBlobIdLength = stream.readShort();
        String latestBlobId = null;
        if (latestBlobIdLength != 0) {
          byte[] latestBlobIdbytes = new byte[latestBlobIdLength];
          stream.read(latestBlobIdbytes, 0, (int) latestBlobIdLength);
          latestBlobId = Arrays.toString(latestBlobIdbytes);
        }
        cloudFindToken = new CloudFindToken(version, latestUploadTime, latestBlobId, bytesRead);
        break;
      case VERSION_3:
        FindTokenType type = FindTokenType.values()[stream.readShort()];
        latestUploadTime = stream.readLong();
        bytesRead = stream.readLong();
        latestBlobIdLength = stream.readShort();
        latestBlobId = null;
        if (latestBlobIdLength != 0) {
          byte[] latestBlobIdbytes = new byte[latestBlobIdLength];
          stream.read(latestBlobIdbytes, 0, (int) latestBlobIdLength);
          latestBlobId = Arrays.toString(latestBlobIdbytes);
        }
        cloudFindToken = new CloudFindToken(version, latestUploadTime, latestBlobId, bytesRead);
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

  public long getLatestUploadTime() {
    return latestUploadTime;
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
    return version == that.version && latestUploadTime == that.latestUploadTime && bytesRead == that.bytesRead
        && Objects.equals(latestBlobId, that.latestBlobId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, latestUploadTime, latestBlobId, bytesRead);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version: ").append(version);
    sb.append(" latestUploadTime: ").append(latestUploadTime);
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
