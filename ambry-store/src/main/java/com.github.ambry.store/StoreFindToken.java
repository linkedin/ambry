/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * The StoreFindToken is an implementation of FindToken.
 * It is used to provide a token to the client to resume
 * the find from where it was left previously. The StoreFindToken
 * maintains an offset to track entries within the journal. If the
 * offset gets outside the range of the journal, the storekey and
 * indexstartoffset that refers to the segment of the index is used
 * to perform the search. This is possible because the journal is
 * always equal or larger than the writable segment.
 */
public class StoreFindToken implements FindToken {
  /**
   * The type of the store token.
   */
  public enum Type {
    Uninitialized, JournalBased, IndexBased
  }

  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;

  private static final int VERSION_SIZE = 2;
  private static final int TYPE_SIZE = 2;
  private static final int SESSION_ID_LENGTH_SIZE = 4;

  private static final byte[] ZERO_LENGTH_ARRAY = new byte[0];
  private static final int UNINITIALIZED_OFFSET = -1;

  private final Type type;
  private final Offset offset;
  private final StoreKey storeKey;
  private final UUID sessionId;
  private long bytesRead;

  StoreFindToken() {
    this(Type.Uninitialized, null, null, null);
  }

  StoreFindToken(StoreKey key, Offset indexSegmentStartOffset, UUID sessionId) {
    this(Type.IndexBased, indexSegmentStartOffset, key, sessionId);
  }

  StoreFindToken(Offset offset, UUID sessionId) {
    this(Type.JournalBased, offset, null, sessionId);
  }

  private StoreFindToken(Type type, Offset offset, StoreKey key, UUID sessionId) {
    if (!type.equals(Type.Uninitialized)) {
      if (offset == null || sessionId == null) {
        throw new IllegalArgumentException("Offset [" + offset + "] or SessionId [" + sessionId + "] cannot be null");
      } else if (type.equals(Type.IndexBased) && key == null) {
        throw new IllegalArgumentException("StoreKey cannot be null for an index based token");
      }
    }
    this.type = type;
    this.offset = offset;
    this.storeKey = key;
    this.sessionId = sessionId;
    this.bytesRead = -1;
  }

  void setBytesRead(long bytesRead) {
    this.bytesRead = bytesRead;
  }

  static StoreFindToken fromBytes(DataInputStream stream, StoreKeyFactory factory) throws IOException {
    StoreFindToken storeFindToken;
    // read version
    short version = stream.readShort();
    switch (version) {
      case VERSION_0:
        // backwards compatibility
        String logSegmentName = LogSegmentNameHelper.generateFirstSegmentName(1);
        // read sessionId
        String sessionId = Utils.readIntString(stream);
        UUID sessionIdUUID = null;
        if (!sessionId.isEmpty()) {
          sessionIdUUID = UUID.fromString(sessionId);
        }
        // read offset
        long offset = stream.readLong();
        // read index start offset
        long indexStartOffset = stream.readLong();
        if (indexStartOffset != UNINITIALIZED_OFFSET) {
          // read store key if needed
          storeFindToken = new StoreFindToken(factory.getStoreKey(stream), new Offset(logSegmentName, indexStartOffset),
              sessionIdUUID);
        } else if (offset != UNINITIALIZED_OFFSET) {
          storeFindToken = new StoreFindToken(new Offset(logSegmentName, offset), sessionIdUUID);
        } else {
          storeFindToken = new StoreFindToken();
        }
        break;
      case VERSION_1:
        // read sessionId
        sessionId = Utils.readIntString(stream);
        sessionIdUUID = null;
        if (!sessionId.isEmpty()) {
          sessionIdUUID = UUID.fromString(sessionId);
        }
        // read type
        Type type = Type.values()[stream.readShort()];
        switch (type) {
          case Uninitialized:
            storeFindToken = new StoreFindToken();
            break;
          case JournalBased:
            Offset logOffset = Offset.fromBytes(stream);
            storeFindToken = new StoreFindToken(logOffset, sessionIdUUID);
            break;
          case IndexBased:
            Offset indexSegmentStartOffset = Offset.fromBytes(stream);
            storeFindToken = new StoreFindToken(factory.getStoreKey(stream), indexSegmentStartOffset, sessionIdUUID);
            break;
          default:
            throw new IllegalStateException("Unknown store find token type: " + type);
        }
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version in StoreFindToken: " + version);
    }
    return storeFindToken;
  }

  public Type getType() {
    return type;
  }

  public StoreKey getStoreKey() {
    return storeKey;
  }

  public Offset getOffset() {
    return offset;
  }

  public UUID getSessionId() {
    return sessionId;
  }

  @Override
  public long getBytesRead() {
    if (this.bytesRead == -1) {
      throw new IllegalStateException("Bytes read not initialized");
    }
    return this.bytesRead;
  }

  @Override
  public byte[] toBytes() {
    byte[] offsetBytes = offset != null ? offset.toBytes() : ZERO_LENGTH_ARRAY;
    byte[] sessionIdBytes = sessionId != null ? sessionId.toString().getBytes() : ZERO_LENGTH_ARRAY;
    byte[] storeKeyBytes = storeKey != null ? storeKey.toBytes() : ZERO_LENGTH_ARRAY;
    int size = VERSION_SIZE + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length + TYPE_SIZE + offsetBytes.length
        + storeKeyBytes.length;
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort(VERSION_1);
    // add sessionId
    bufWrap.putInt(sessionIdBytes.length);
    bufWrap.put(sessionIdBytes);
    // add type
    bufWrap.putShort((short) type.ordinal());
    // add offset
    bufWrap.put(offsetBytes);
    // add StoreKey
    bufWrap.put(storeKeyBytes);
    return buf;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("type: ").append(type);
    if (!type.equals(Type.Uninitialized)) {
      if (sessionId != null) {
        sb.append(" sessionId ").append(sessionId);
      }
      if (storeKey != null) {
        sb.append(" storeKey ").append(storeKey);
      }
      sb.append(" offset ").append(offset);
      sb.append(" bytesRead ").append(bytesRead);
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StoreFindToken that = (StoreFindToken) o;

    if (type != that.type) {
      return false;
    }
    if (offset != null ? !offset.equals(that.offset) : that.offset != null) {
      return false;
    }
    return storeKey != null ? storeKey.equals(that.storeKey) : that.storeKey == null;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    result = 31 * result + (storeKey != null ? storeKey.hashCode() : 0);
    return result;
  }
}
