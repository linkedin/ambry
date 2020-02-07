/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * {@link FindToken} object to act as bookmark for replication based on Cosmos update time field.
 */
public class CosmosUpdateTimeFindToken implements FindToken {

  static final short VERSION_0 = 0;
  static final short CURRENT_VERSION = VERSION_0;
  private final short version;
  private final FindTokenType type;
  private final long lastUpdateTime;
  private final Set<String> lastUpdateTimeReadBlobIds;
  private final long bytesRead;

  /** Constructor for start token */
  public CosmosUpdateTimeFindToken() {
    this((short) 0, 0, new HashSet<>());
  }

  /** Constructor for in-progress token */
  public CosmosUpdateTimeFindToken(long lastUpdateTime, long bytesRead, Set<String> lastUpdateTimeReadBlobIds) {
    this.version = CURRENT_VERSION;
    this.type = FindTokenType.CloudBased;
    this.lastUpdateTime = lastUpdateTime;
    this.bytesRead = bytesRead;
    this.lastUpdateTimeReadBlobIds = new HashSet<>(lastUpdateTimeReadBlobIds);
  }

  /** Constructor for reading token that can have older version */
  public CosmosUpdateTimeFindToken(short version, long lastUpdateTime, long bytesRead,
      Set<String> lastUpdateTimeReadBlobIds) {
    this.version = version;
    this.type = FindTokenType.CloudBased;
    this.lastUpdateTime = lastUpdateTime;
    this.bytesRead = bytesRead;
    this.lastUpdateTimeReadBlobIds = new HashSet<>(lastUpdateTimeReadBlobIds);
  }

  /**
   * Utility to construct a new CloudFindToken from a previous instance and the results of a findEntriesSince query.
   * @param prevToken previous {@link CosmosUpdateTimeFindToken}.
   * @param queryResults List of {@link CloudBlobMetadata} objects.
   * @return the updated token.
   */
  public static CosmosUpdateTimeFindToken getUpdatedToken(CosmosUpdateTimeFindToken prevToken,
      List<CloudBlobMetadata> queryResults) {
    if (queryResults.isEmpty()) {
      return prevToken;
    }

    Set<String> lastUpdateTimeReadBlobIds;
    long lastUpdateTime = queryResults.get(queryResults.size() - 1).getLastUpdateTime();
    if (lastUpdateTime == prevToken.getLastUpdateTime()) {
      // If last update time doesn't progress with the new token, then new token should include all the previous blobIds
      // with the same last update time.
      lastUpdateTimeReadBlobIds = new HashSet<>(prevToken.getLastUpdateTimeReadBlobIds());
    } else {
      lastUpdateTimeReadBlobIds = new HashSet<>();
    }

    for (int i = queryResults.size() - 1; i >= 0; i--) {
      if (queryResults.get(i).getLastUpdateTime() == lastUpdateTime) {
        lastUpdateTimeReadBlobIds.add(queryResults.get(i).getId());
      } else {
        break;
      }
    }

    long bytesReadThisQuery = queryResults.stream().mapToLong(CloudBlobMetadata::getSize).sum();
    return new CosmosUpdateTimeFindToken(lastUpdateTime, prevToken.getBytesRead() + bytesReadThisQuery,
        lastUpdateTimeReadBlobIds);
  }

  @Override
  public byte[] toBytes() {
    byte[] buf;
    switch (version) {
      case VERSION_0:
        int size = 2 * Short.BYTES + 2 * Long.BYTES + Short.BYTES;
        for (String blobId : lastUpdateTimeReadBlobIds) {
          size += Short.BYTES; //for size of string
          size += blobId.length(); //for the string itself
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
        // add lastUpdateTimeReadBlobIds
        bufWrap.putShort((short) lastUpdateTimeReadBlobIds.size());
        for (String blobId : lastUpdateTimeReadBlobIds) {
          bufWrap.putShort((short) blobId.length());
          bufWrap.put(blobId.getBytes());
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
  static CosmosUpdateTimeFindToken fromBytes(DataInputStream inputStream) throws IOException {
    CosmosUpdateTimeFindToken cloudFindToken;
    DataInputStream stream = new DataInputStream(inputStream);
    short version = stream.readShort();
    switch (version) {
      case VERSION_0:
        FindTokenType type = FindTokenType.values()[stream.readShort()];
        long lastUpdateTime = stream.readLong();
        long bytesRead = stream.readLong();
        int numBlobs = stream.readShort();
        Set<String> blobIds = new HashSet<>();
        while (numBlobs > 0) {
          int blobIdLength = stream.readShort();
          byte[] blobIdBytes = new byte[blobIdLength];
          stream.read(blobIdBytes, 0, blobIdLength);
          blobIds.add(new String(blobIdBytes));
          numBlobs--;
        }
        cloudFindToken = new CosmosUpdateTimeFindToken(version, lastUpdateTime, bytesRead, blobIds);
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

  /**
   * Return {@code lastUpdateTime}.
   * @return {@code lastUpdateTime}
   */
  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  /**
   * Return {@code lastUpdateTimeReadBlobIds}
   * @return {@code lastUpdateTimeReadBlobIds}
   */
  public Set<String> getLastUpdateTimeReadBlobIds() {
    return lastUpdateTimeReadBlobIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CosmosUpdateTimeFindToken that = (CosmosUpdateTimeFindToken) o;
    return version == that.version && lastUpdateTime == that.lastUpdateTime && bytesRead == that.bytesRead
        && lastUpdateTimeReadBlobIds.equals(that.lastUpdateTimeReadBlobIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, lastUpdateTime, bytesRead, lastUpdateTimeReadBlobIds);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version: ").append(version);
    sb.append(" lastUpdateTime: ").append(lastUpdateTime);
    sb.append(" bytesRead: ").append(bytesRead);
    sb.append(" lastUpdateTimeReadBlobIds: ").append(lastUpdateTimeReadBlobIds);
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
