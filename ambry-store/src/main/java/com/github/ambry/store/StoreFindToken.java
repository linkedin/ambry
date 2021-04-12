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

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
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

  static final short VERSION_0 = 0;
  static final short VERSION_1 = 1;
  static final short VERSION_2 = 2;
  static final short VERSION_3 = 3;
  static final short CURRENT_VERSION = VERSION_3;

  static final short UNINITIALIZED_RESET_KEY_VERSION = -1;
  static final short RESET_KEY_VERSION_SIZE = 2;
  static final int VERSION_SIZE = 2;
  static final int TYPE_SIZE = 2;
  static final int RESET_KEY_TYPE_SIZE = 2;
  static final int SESSION_ID_LENGTH_SIZE = 4;
  static final int INCARNATION_ID_LENGTH_SIZE = 4;
  static final int INCLUSIVE_BYTE_SIZE = 1;
  static final int HAS_RESET_KEY_INFO_BYTE_SIZE = 1;

  private static final byte[] ZERO_LENGTH_ARRAY = new byte[0];
  private static final int UNINITIALIZED_OFFSET = -1;

  // refers to the type of the token
  private final FindTokenType type;
  // refers to the offset in the log. Could be either of Journal or Index based token
  private final Offset offset;
  // refers to the store key in case of Index based token
  // in case of journal based token, represents if the blob at the offset(of the token)
  private final StoreKey storeKey;
  // refers to the store key lies at index segment start offset in the log (used to find valid start point if previous
  // index segment has been compacted)
  private final StoreKey resetKey;
  // the {@link PersistentIndex.IndexEntryType} associated with this reset key.
  private final PersistentIndex.IndexEntryType resetKeyType;
  private final short resetKeyVersion;
  // is inclusive or not
  private final byte inclusive;
  private final byte hasResetKeyInfo;
  // refers to the sessionId of the store. On every restart a new sessionId is created
  private final UUID sessionId;
  // refers to the incarnationId of the store. On every re-creation of the store, a new incarnationId is created
  private final UUID incarnationId;
  // refers to the bytes read so far (from the beginning of the log)
  private long bytesRead;
  // refers to the version of the StoreFindToken
  private final short version;

  /**
   * Uninitialized token. Refers to the starting of the log.
   */
  StoreFindToken() {
    this(FindTokenType.Uninitialized, null, null, null, null, true, CURRENT_VERSION, null, null,
        UNINITIALIZED_RESET_KEY_VERSION);
  }

  /**
   * Index based token. Refers to an index segment start offset and a store key that belongs to that index segment. The
   * token formats with different versions are as follows.
   * VERSION_1
   * +----------------------------------------------------------------------------+
   * | version | session id | token type | index segment start offset | store key |
   * | (short) | (n bytes)  |  (short)   |        (n bytes)           | (n bytes) |
   * +----------------------------------------------------------------------------+
   *
   * VERSION_2
   * +---------------------------------------------------------------------------------------------+
   * | version | token type | incarnation id | session id | index segment start offset | store key |
   * | (short) |  (short)   |  (n bytes)     |  (n bytes) |        (n bytes)           | (n bytes) |
   * +---------------------------------------------------------------------------------------------+
   *
   * VERSION_3
   * +-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   * | version | token type | incarnation id | session id | index segment start offset | store key | has reset key info | reset key | reset key type | reset key version |
   * | (short) |  (short)   |  (n bytes)     |  (n bytes) |        (n bytes)           | (n bytes) |      (byte)        | (n bytes) |    (short)     |     (short)       |
   * +-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   * @param key The {@link StoreKey} which the token refers to. Index segments are keyed on store keys and hence
   * @param indexSegmentStartOffset the start offset of the index segment which the token refers to
   * @param sessionId the sessionId of the store
   * @param incarnationId the incarnationId of the store
   * @param resetKey The {@link StoreKey} at start offset of current index segment. When log segment on source node is
   *                 compacted (and token becomes invalid), this key is used to find current valid log segment where
   *                 {@link PersistentIndex#findEntriesSince(FindToken, long)} can start from.
   * @param resetKeyType The {@link PersistentIndex.IndexEntryType} associated with this reset key.
   * @param resetKeyVersion The life version of reset key.
   */
  StoreFindToken(StoreKey key, Offset indexSegmentStartOffset, UUID sessionId, UUID incarnationId, StoreKey resetKey,
      PersistentIndex.IndexEntryType resetKeyType, short resetKeyVersion) {
    this(FindTokenType.IndexBased, indexSegmentStartOffset, key, sessionId, incarnationId, false, CURRENT_VERSION,
        resetKey, resetKeyType, resetKeyVersion);
  }

  /**
   * Journal based token. Refers to an offset in the journal. The token formats with different versions are as follows.
   * VERSION_1
   * +------------------------------------------------------------+
   * | version | session id | token type | log offset | store key |
   * | (short) | (n bytes)  |  (short)   | (n bytes)  | (n bytes) |
   * +------------------------------------------------------------+
   *
   * VERSION_2
   * +-----------------------------------------------------------------------------+
   * | version | token type | incarnation id | session id | log offset | inclusive |
   * | (short) |  (short)   |   (n bytes)    | (n bytes)  | (n bytes)  |  (byte)   |
   * +-----------------------------------------------------------------------------+
   *
   * VERSION_3
   * +---------------------------------------------------------------------------------------------------------------------------------------------------+
   * | version | token type | incarnation id | session id | log offset | inclusive | has reset key info | reset key | reset key type | reset key version |
   * | (short) |  (short)   |   (n bytes)    | (n bytes)  | (n bytes)  |  (byte)   |      (byte)        | (n bytes) |    (short)     |     (short)       |
   * +---------------------------------------------------------------------------------------------------------------------------------------------------+
   * @param offset the offset that this token refers to in the journal
   * @param sessionId the sessionId of the store
   * @param incarnationId the incarnationId of the store
   * @param inclusive {@code true} if the offset is inclusive or in other words the blob at the given offset is inclusive.
   *                  {@code false} otherwise
   * @param resetKey The {@link StoreKey} at start offset of current index segment. When log segment on source node is
   *                 compacted (and token becomes invalid), this key is used to find current valid log segment where
   *                 {@link PersistentIndex#findEntriesSince(FindToken, long)} can start from.
   * @param resetKeyType The {@link PersistentIndex.IndexEntryType} associated with this reset key.
   * @param resetKeyVersion The life version of reset key.
   */
  StoreFindToken(Offset offset, UUID sessionId, UUID incarnationId, boolean inclusive, StoreKey resetKey,
      PersistentIndex.IndexEntryType resetKeyType, short resetKeyVersion) {
    this(FindTokenType.JournalBased, offset, null, sessionId, incarnationId, inclusive, CURRENT_VERSION, resetKey,
        resetKeyType, resetKeyVersion);
  }

  /**
   * Instantiating {@link StoreFindToken}
   * @param type the {@link FindTokenType} of the token
   * @param offset the offset that this token refers to
   * @param key The {@link StoreKey} that the token refers to
   * @param sessionId the sessionId of the store that this token refers to
   * @param incarnationId the incarnationId of the store that this token refers to
   * @param inclusive {@code true} if the offset is inclusive or in other words the blob at the given offset is inclusive.
   *                  {@code false} otherwise
   * @param version refers to the version of the token
   * @param resetKey The {@link StoreKey} at start offset of current index segment. When log segment on source node is
   *                 compacted (and token becomes invalid), this key is used to find current valid log segment where
   *                 {@link PersistentIndex#findEntriesSince(FindToken, long)} can start from.
   * @param resetKeyType The {@link PersistentIndex.IndexEntryType} associated with this reset key.
   * @param resetKeyVersion The life version of reset key.
   */
  StoreFindToken(FindTokenType type, Offset offset, StoreKey key, UUID sessionId, UUID incarnationId, boolean inclusive,
      short version, StoreKey resetKey, PersistentIndex.IndexEntryType resetKeyType, short resetKeyVersion) {
    if (!type.equals(FindTokenType.Uninitialized)) {
      if (offset == null || sessionId == null) {
        throw new IllegalArgumentException("Offset [" + offset + "] or SessionId [" + sessionId + "] cannot be null");
      } else if (type.equals(FindTokenType.IndexBased) && key == null) {
        throw new IllegalArgumentException("StoreKey cannot be null for an index based token");
      }
      if (version >= VERSION_2 && incarnationId == null) {
        throw new IllegalArgumentException("IncarnationId cannot be null for StoreFindToken of version 2");
      }
      if (version >= VERSION_3 && (resetKey != null && resetKeyType == null
          || resetKey == null && resetKeyType != null)) {
        throw new IllegalArgumentException("Among reset key and reset key type, one is null but the other exists");
      }
    }
    this.type = type;
    this.offset = offset;
    this.storeKey = key;
    this.sessionId = sessionId;
    this.inclusive = inclusive ? (byte) 1 : 0;
    this.incarnationId = incarnationId;
    this.bytesRead = -1;
    this.version = version;
    this.resetKey = resetKey;
    this.resetKeyType = resetKeyType;
    this.resetKeyVersion = resetKeyVersion;
    this.hasResetKeyInfo = resetKey == null ? (byte) 0 : 1;
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
        LogSegmentName logSegmentName = LogSegmentName.generateFirstSegmentName(false);
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
          storeFindToken = new StoreFindToken(FindTokenType.IndexBased, new Offset(logSegmentName, indexStartOffset),
              factory.getStoreKey(stream), sessionIdUUID, null, false, VERSION_0, null, null,
              UNINITIALIZED_RESET_KEY_VERSION);
        } else if (offset != UNINITIALIZED_OFFSET) {
          storeFindToken =
              new StoreFindToken(FindTokenType.JournalBased, new Offset(logSegmentName, offset), null, sessionIdUUID,
                  null, false, VERSION_0, null, null, UNINITIALIZED_RESET_KEY_VERSION);
        } else {
          storeFindToken =
              new StoreFindToken(FindTokenType.Uninitialized, null, null, null, null, true, VERSION_0, null, null,
                  UNINITIALIZED_RESET_KEY_VERSION);
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
        FindTokenType type = FindTokenType.values()[stream.readShort()];
        switch (type) {
          case Uninitialized:
            storeFindToken =
                new StoreFindToken(FindTokenType.Uninitialized, null, null, null, null, true, VERSION_1, null, null,
                    UNINITIALIZED_RESET_KEY_VERSION);
            break;
          case JournalBased:
            Offset logOffset = Offset.fromBytes(stream);
            storeFindToken =
                new StoreFindToken(FindTokenType.JournalBased, logOffset, null, sessionIdUUID, null, false, VERSION_1,
                    null, null, UNINITIALIZED_RESET_KEY_VERSION);
            break;
          case IndexBased:
            Offset indexSegmentStartOffset = Offset.fromBytes(stream);
            storeFindToken =
                new StoreFindToken(FindTokenType.IndexBased, indexSegmentStartOffset, factory.getStoreKey(stream),
                    sessionIdUUID, null, false, VERSION_1, null, null, UNINITIALIZED_RESET_KEY_VERSION);
            break;
          default:
            throw new IllegalStateException("Unknown store find token type: " + type);
        }
        break;
      case VERSION_2:
        // read type
        type = FindTokenType.values()[stream.readShort()];
        switch (type) {
          case Uninitialized:
            storeFindToken =
                new StoreFindToken(FindTokenType.Uninitialized, null, null, null, null, true, VERSION_2, null, null,
                    UNINITIALIZED_RESET_KEY_VERSION);
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
            storeFindToken =
                new StoreFindToken(FindTokenType.JournalBased, logOffset, null, sessionIdUUID, incarnationIdUUID,
                    inclusive == (byte) 1, VERSION_2, null, null, UNINITIALIZED_RESET_KEY_VERSION);
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
            storeFindToken =
                new StoreFindToken(FindTokenType.IndexBased, indexSegmentStartOffset, storeKey, sessionIdUUID,
                    incarnationIdUUID, false, VERSION_2, null, null, UNINITIALIZED_RESET_KEY_VERSION);
            break;
          default:
            throw new IllegalStateException("Unknown store find token type: " + type);
        }
        break;
      case VERSION_3:
        // read type
        type = FindTokenType.values()[stream.readShort()];
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
            byte hasRestKeyInfo = stream.readByte();
            // read reset key (if present)
            StoreKey journalTokenResetKey = hasRestKeyInfo == (byte) 1 ? factory.getStoreKey(stream) : null;
            // read reset key type (if present)
            PersistentIndex.IndexEntryType journalResetKeyType = hasRestKeyInfo == (byte) 1 ?
                PersistentIndex.IndexEntryType.values()[stream.readShort()] : null;
            // read reset key life version (if present)
            short journalResetKeyVersion =
                hasRestKeyInfo == (byte) 1 ? stream.readShort() : UNINITIALIZED_RESET_KEY_VERSION;
            storeFindToken = new StoreFindToken(logOffset, sessionIdUUID, incarnationIdUUID, inclusive == (byte) 1,
                journalTokenResetKey, journalResetKeyType, journalResetKeyVersion);
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
            hasRestKeyInfo = stream.readByte();
            // read reset key (if present)
            StoreKey indexTokenResetKey = hasRestKeyInfo == (short) 1 ? factory.getStoreKey(stream) : null;
            // read reset key type (if present)
            PersistentIndex.IndexEntryType indexResetKeyType =
                hasRestKeyInfo == (short) 1 ? PersistentIndex.IndexEntryType.values()[stream.readShort()] : null;
            // read reset key life version (if present)
            short indexResetKeyVersion =
                hasRestKeyInfo == (short) 1 ? stream.readShort() : UNINITIALIZED_RESET_KEY_VERSION;
            storeFindToken = new StoreFindToken(storeKey, indexSegmentStartOffset, sessionIdUUID, incarnationIdUUID,
                indexTokenResetKey, indexResetKeyType, indexResetKeyVersion);
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

  public FindTokenType getType() {
    return type;
  }

  public StoreKey getStoreKey() {
    return storeKey;
  }

  public StoreKey getResetKey() {
    return resetKey;
  }

  public PersistentIndex.IndexEntryType getResetKeyType() {
    return resetKeyType;
  }

  public short getResetKeyVersion() {
    return resetKeyVersion;
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

  boolean hasResetKeyInfo() {
    return hasResetKeyInfo == (byte) 1;
  }

  @Override
  public short getVersion() {
    return version;
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
    byte[] buf = null;
    switch (version) {
      case VERSION_0:
        int offsetSize = 8;
        int startOffsetSize = 8;
        byte[] sessionIdBytes = getSessionIdInBytes();
        byte[] storeKeyBytes = getStoreKeyInBytes();
        int size = VERSION_SIZE + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length + offsetSize + startOffsetSize
            + storeKeyBytes.length;
        buf = new byte[size];
        ByteBuffer bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(version);
        // add sessionId
        bufWrap.putInt(sessionIdBytes.length);
        bufWrap.put(sessionIdBytes);
        // add offset for journal based token
        bufWrap.putLong((type == FindTokenType.JournalBased) ? offset.getOffset() : UNINITIALIZED_OFFSET);
        // add index start offset for Index based token
        bufWrap.putLong((type == FindTokenType.IndexBased) ? offset.getOffset() : UNINITIALIZED_OFFSET);
        // add store key
        bufWrap.put(storeKeyBytes);
        break;
      case VERSION_1:
        byte[] offsetBytes = getOffsetInBytes();
        sessionIdBytes = getSessionIdInBytes();
        storeKeyBytes = getStoreKeyInBytes();
        size = VERSION_SIZE + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length + TYPE_SIZE + offsetBytes.length
            + storeKeyBytes.length;
        buf = new byte[size];
        bufWrap = ByteBuffer.wrap(buf);
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
        break;
      case VERSION_2:
        offsetBytes = getOffsetInBytes();
        sessionIdBytes = getSessionIdInBytes();
        byte[] incarnationIdBytes = getIncarnationIdInBytes();
        storeKeyBytes = getStoreKeyInBytes();
        size = VERSION_SIZE + TYPE_SIZE;
        if (type != FindTokenType.Uninitialized) {
          size +=
              INCARNATION_ID_LENGTH_SIZE + incarnationIdBytes.length + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length
                  + offsetBytes.length;
          if (type == FindTokenType.JournalBased) {
            size += INCLUSIVE_BYTE_SIZE;
          } else if (type == FindTokenType.IndexBased) {
            size += storeKeyBytes.length;
          }
        }
        buf = new byte[size];
        bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(VERSION_2);
        // add type
        bufWrap.putShort((short) type.ordinal());
        if (type != FindTokenType.Uninitialized) {
          // add incarnationId
          bufWrap.putInt(incarnationIdBytes.length);
          bufWrap.put(incarnationIdBytes);
          // add sessionId
          bufWrap.putInt(sessionIdBytes.length);
          bufWrap.put(sessionIdBytes);
          // add offset
          bufWrap.put(offsetBytes);
          if (type == FindTokenType.JournalBased) {
            bufWrap.put(getInclusive() ? (byte) 1 : (byte) 0);
          } else if (type == FindTokenType.IndexBased) {
            bufWrap.put(storeKeyBytes);
          }
        }
        break;
      case VERSION_3:
        offsetBytes = getOffsetInBytes();
        sessionIdBytes = getSessionIdInBytes();
        incarnationIdBytes = getIncarnationIdInBytes();
        storeKeyBytes = getStoreKeyInBytes();
        byte[] resetKeyBytes = getResetKeyInBytes();
        size = VERSION_SIZE + TYPE_SIZE;
        if (type != FindTokenType.Uninitialized) {
          size +=
              INCARNATION_ID_LENGTH_SIZE + incarnationIdBytes.length + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length
                  + offsetBytes.length;
          if (type == FindTokenType.JournalBased) {
            size += INCLUSIVE_BYTE_SIZE;
          } else if (type == FindTokenType.IndexBased) {
            size += storeKeyBytes.length;
          }
          size += HAS_RESET_KEY_INFO_BYTE_SIZE;
          if (hasResetKeyInfo()) {
            size += resetKeyBytes.length + RESET_KEY_TYPE_SIZE + RESET_KEY_VERSION_SIZE;
          }
        }
        buf = new byte[size];
        bufWrap = ByteBuffer.wrap(buf);
        // add version
        bufWrap.putShort(VERSION_3);
        // add type
        bufWrap.putShort((short) type.ordinal());
        if (type != FindTokenType.Uninitialized) {
          // add incarnationId
          bufWrap.putInt(incarnationIdBytes.length);
          bufWrap.put(incarnationIdBytes);
          // add sessionId
          bufWrap.putInt(sessionIdBytes.length);
          bufWrap.put(sessionIdBytes);
          // add offset
          bufWrap.put(offsetBytes);
          if (type == FindTokenType.JournalBased) {
            bufWrap.put(getInclusive() ? (byte) 1 : (byte) 0);
          } else if (type == FindTokenType.IndexBased) {
            bufWrap.put(storeKeyBytes);
          }
          bufWrap.put(hasResetKeyInfo() ? (byte) 1 : (byte) 0);
          if (hasResetKeyInfo()) {
            // add reset key
            bufWrap.put(resetKeyBytes);
            // add reset key type
            bufWrap.putShort((short) resetKeyType.ordinal());
            // add reset key version
            bufWrap.putShort(resetKeyVersion);
          }
        }
        break;
    }
    return buf;
  }

  byte[] getOffsetInBytes() {
    return offset != null ? offset.toBytes() : ZERO_LENGTH_ARRAY;
  }

  byte[] getStoreKeyInBytes() {
    return storeKey != null ? storeKey.toBytes() : ZERO_LENGTH_ARRAY;
  }

  byte[] getResetKeyInBytes() {
    return resetKey != null ? resetKey.toBytes() : ZERO_LENGTH_ARRAY;
  }

  byte[] getSessionIdInBytes() {
    return sessionId != null ? sessionId.toString().getBytes() : ZERO_LENGTH_ARRAY;
  }

  byte[] getIncarnationIdInBytes() {
    return incarnationId != null ? incarnationId.toString().getBytes() : ZERO_LENGTH_ARRAY;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version: ").append(version);
    sb.append(" type: ").append(type);
    if (incarnationId != null) {
      sb.append(" incarnationId ").append(incarnationId);
    }
    sb.append(" inclusiveness ").append(inclusive == 1);
    if (!type.equals(FindTokenType.Uninitialized)) {
      if (sessionId != null) {
        sb.append(" sessionId ").append(sessionId);
      }
      if (storeKey != null) {
        sb.append(" storeKey ").append(storeKey);
      }
      if (resetKey != null) {
        sb.append(" resetKey ").append(resetKey);
        sb.append(" resetKeyType ").append(resetKeyType);
        sb.append(" resetKeyVersion ").append(resetKeyVersion);
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

    return type == that.type && Objects.equals(offset, that.offset) && inclusive == that.inclusive && Objects.equals(
        storeKey, that.storeKey) && Objects.equals(resetKey, that.resetKey) && Objects.equals(resetKeyType,
        that.resetKeyType) && Objects.equals(resetKeyVersion, that.resetKeyVersion);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    result = 31 * result + inclusive;
    result = 31 * result + (storeKey != null ? storeKey.hashCode() : 0);
    result = 31 * result + (resetKey != null ? resetKey.hashCode() : 0);
    result = 31 * result + (resetKeyType != null ? resetKeyType.hashCode() : 0);
    result = 31 * result + (resetKeyVersion != UNINITIALIZED_RESET_KEY_VERSION ? Short.hashCode(resetKeyVersion) : 0);
    return result;
  }
}
