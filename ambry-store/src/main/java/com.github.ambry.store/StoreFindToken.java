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
  static final short VERSION_2 = 2;

  private static final int VERSION_SIZE = 2;
  private static final int TYPE_SIZE = 2;
  private static final int SESSION_ID_LENGTH_SIZE = 4;
  private static final int INCARNATION_ID_LENGTH_SIZE = 4;
  private static final int INCLUSIVE_BYTE_SIZE = 1;

  private static final byte[] ZERO_LENGTH_ARRAY = new byte[0];
  private static final int UNINITIALIZED_OFFSET = -1;
  // refers to the type of the token
  private final Type type;
  // refers to the offset in the log. Could be either of Journal or Index based token
  private final Offset offset;
  // refers to the store key incase of Index based token
  // incase of journal based token, represents if the blob at the offset(of the token)
  private final StoreKey storeKey;
  // is inclusive or not
  private final byte inclusive;
  // refers to the sessionId of the store. On every restart a new sessionId is created
  private final UUID sessionId;
  // refers to the incarnationId of the store. On every re-creation of the store, a new incarnationId is created
  private final UUID incarnationId;
  // refers to the bytes read so far (from the beginning of the log)
  private long bytesRead;

  /**
   * Uninitialized token. Refers to the starting of the log.
   */
  StoreFindToken() {
    this(Type.Uninitialized, null, null, null, null, true);
  }

  /**
   * Index based token. Refers to an index segment start offset and a store key that belongs to that index segment
   * @param key The {@link StoreKey} which the token refers to. Index segments are keyed on store keys and hence
   * @param indexSegmentStartOffset the start offset of the index segment which the token refers to
   * @param sessionId the sessionId of the store
   * @param incarnationId the incarnationId of the store
   */
  StoreFindToken(StoreKey key, Offset indexSegmentStartOffset, UUID sessionId, UUID incarnationId) {
    this(Type.IndexBased, indexSegmentStartOffset, key, sessionId, incarnationId, false);
  }

  /**
   * Journal based token. Refers to an offset in the journal
   * @param offset the offset that this token refers to in the journal
   * @param sessionId the sessionId of the store
   * @param incarnationId the incarnationId of the store
   * @param inclusive {@code true} if the offset is inclusive or in other words the blob at the given offset is inclusive.
   *                  {@code false} otherwise
   */
  StoreFindToken(Offset offset, UUID sessionId, UUID incarnationId, boolean inclusive) {
    this(Type.JournalBased, offset, null, sessionId, incarnationId, inclusive);
  }

  private StoreFindToken(Type type, Offset offset, StoreKey key, UUID sessionId, UUID incarnationId,
      boolean inclusive) {
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
    this.inclusive = inclusive ? (byte) 1 : 0;
    this.incarnationId = incarnationId;
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
              sessionIdUUID, null);
        } else if (offset != UNINITIALIZED_OFFSET) {
          storeFindToken = new StoreFindToken(new Offset(logSegmentName, offset), sessionIdUUID, null, false);
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
            storeFindToken = new StoreFindToken(logOffset, sessionIdUUID, null, false);
            break;
          case IndexBased:
            Offset indexSegmentStartOffset = Offset.fromBytes(stream);
            storeFindToken =
                new StoreFindToken(factory.getStoreKey(stream), indexSegmentStartOffset, sessionIdUUID, null);
            break;
          default:
            throw new IllegalStateException("Unknown store find token type: " + type);
        }
        break;
      case VERSION_2:
        // read type
        type = Type.values()[stream.readShort()];
        switch (type) {
          case Uninitialized:
            storeFindToken = new StoreFindToken();
            break;
          case JournalBased:
            // read incarnationId
            String incarnationId = Utils.readIntString(stream);
            UUID incarnationIdUUID = UUID.fromString(incarnationId);
            // read sessionId
            sessionId = Utils.readIntString(stream);
            sessionIdUUID = UUID.fromString(sessionId);
            Offset logOffset = Offset.fromBytes(stream);
            byte inclusive = stream.readByte();
            storeFindToken = new StoreFindToken(logOffset, sessionIdUUID, incarnationIdUUID, inclusive == (byte) 1);
            break;
          case IndexBased:
            // read incarnationId
            incarnationId = Utils.readIntString(stream);
            incarnationIdUUID = UUID.fromString(incarnationId);
            // read sessionId
            sessionId = Utils.readIntString(stream);
            sessionIdUUID = UUID.fromString(sessionId);
            Offset indexSegmentStartOffset = Offset.fromBytes(stream);
            StoreKey storeKey = factory.getStoreKey(stream);
            storeFindToken = new StoreFindToken(storeKey, indexSegmentStartOffset, sessionIdUUID, incarnationIdUUID);
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

  UUID getSessionId() {
    return sessionId;
  }

  UUID getIncarnationId() {
    return incarnationId;
  }

  boolean getInclusive() {
    return inclusive == (byte) 1;
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
    byte[] incarnationIdBytes = incarnationId != null ? incarnationId.toString().getBytes() : ZERO_LENGTH_ARRAY;
    byte[] storeKeyBytes = storeKey != null ? storeKey.toBytes() : ZERO_LENGTH_ARRAY;
    int size = VERSION_SIZE + TYPE_SIZE;
    if (type != Type.Uninitialized) {
      size += INCARNATION_ID_LENGTH_SIZE + incarnationIdBytes.length + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length
          + offsetBytes.length;
      if (type == Type.JournalBased) {
        size += INCLUSIVE_BYTE_SIZE;
      } else if (type == Type.IndexBased) {
        size += storeKeyBytes.length;
      }
    }
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort(VERSION_2);
    // add type
    bufWrap.putShort((short) type.ordinal());
    if (type != Type.Uninitialized) {
      // add incarnationId
      bufWrap.putInt(incarnationIdBytes.length);
      bufWrap.put(incarnationIdBytes);
      // add sessionId
      bufWrap.putInt(sessionIdBytes.length);
      bufWrap.put(sessionIdBytes);
      // add offset
      bufWrap.put(offsetBytes);
      if (type == Type.JournalBased) {
        bufWrap.put(getInclusive() ? (byte) 1 : (byte) 0);
      } else if (type == Type.IndexBased) {
        bufWrap.put(storeKeyBytes);
      }
    }
    return buf;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("type: ").append(type);
    if (incarnationId != null) {
      sb.append(" incarnationId ").append(incarnationId);
    }
    sb.append(" inclusiveness ").append(inclusive == 1 ? true : false);
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
    if (inclusive != that.inclusive) {
      return false;
    }
    return storeKey != null ? storeKey.equals(that.storeKey) : that.storeKey == null;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    result = 31 * result + inclusive;
    result = 31 * result + (storeKey != null ? storeKey.hashCode() : 0);
    return result;
  }
}
