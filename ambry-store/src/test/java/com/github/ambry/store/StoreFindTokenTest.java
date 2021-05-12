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

import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.store.StoreFindToken.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link StoreFindToken}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class StoreFindTokenTest {
  private static final StoreKeyFactory STORE_KEY_FACTORY;
  private final Random random;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final boolean isLogSegmented;

  /**
   * Running for both segmented and non-segmented log.
   * @return an array that returns 1 and 2 (non segmented and segmented)
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public StoreFindTokenTest(boolean isLogSegmented) {
    this.isLogSegmented = isLogSegmented;
    random = new Random();
  }

  /**
   * Tests the correctness of {@link StoreFindToken#equals(Object)}.
   */
  @Test
  public void equalityTest() {
    UUID sessionId = UUID.randomUUID();
    UUID incarnationId = UUID.randomUUID();
    LogSegmentName logSegmentName = LogSegmentName.generateFirstSegmentName(isLogSegmented);
    Offset offset = new Offset(logSegmentName, 0);
    Offset otherOffset = new Offset(logSegmentName, 1);
    MockId key = new MockId(TestUtils.getRandomString(10));
    MockId otherKey = new MockId(TestUtils.getRandomString(10));
    MockId resetKey = new MockId(TestUtils.getRandomString(10));
    PersistentIndex.IndexEntryType resetKeyType =
        PersistentIndex.IndexEntryType.values()[random.nextInt(PersistentIndex.IndexEntryType.values().length)];
    short resetKeyVersion = (short) random.nextInt(5);

    StoreFindToken initToken = new StoreFindToken();
    StoreFindToken otherInitToken = new StoreFindToken();
    StoreFindToken indexToken =
        new StoreFindToken(key, offset, sessionId, incarnationId, resetKey, resetKeyType, resetKeyVersion);
    StoreFindToken otherIndexToken =
        new StoreFindToken(key, offset, sessionId, incarnationId, resetKey, resetKeyType, resetKeyVersion);
    StoreFindToken journalToken =
        new StoreFindToken(offset, sessionId, incarnationId, false, resetKey, resetKeyType, resetKeyVersion);
    StoreFindToken otherJournalToken =
        new StoreFindToken(offset, sessionId, incarnationId, false, resetKey, resetKeyType, resetKeyVersion);
    StoreFindToken inclusiveJournalToken =
        new StoreFindToken(offset, sessionId, incarnationId, true, resetKey, resetKeyType, resetKeyVersion);
    StoreFindToken otherInclusiveJournalToken =
        new StoreFindToken(offset, sessionId, incarnationId, true, resetKey, resetKeyType, resetKeyVersion);

    // equality
    compareTokens(initToken, initToken);
    compareTokens(initToken, otherInitToken);
    compareTokens(indexToken, indexToken);
    compareTokens(indexToken, otherIndexToken);
    compareTokens(journalToken, journalToken);
    compareTokens(journalToken, otherJournalToken);
    compareTokens(inclusiveJournalToken, otherInclusiveJournalToken);

    UUID newSessionId = getRandomUUID(sessionId);
    UUID newIncarnationId = getRandomUUID(incarnationId);

    // equality even if session IDs are different
    compareTokens(indexToken,
        new StoreFindToken(key, offset, newSessionId, incarnationId, resetKey, resetKeyType, resetKeyVersion));
    compareTokens(journalToken,
        new StoreFindToken(offset, newSessionId, incarnationId, false, resetKey, resetKeyType, resetKeyVersion));

    // equality even if incarnation IDs are different
    compareTokens(indexToken,
        new StoreFindToken(key, offset, sessionId, newIncarnationId, resetKey, resetKeyType, resetKeyVersion));
    compareTokens(journalToken,
        new StoreFindToken(offset, sessionId, newIncarnationId, false, resetKey, resetKeyType, resetKeyVersion));

    // inequality if some fields differ
    List<Pair<StoreFindToken, StoreFindToken>> unequalPairs = new ArrayList<>();
    unequalPairs.add(new Pair<>(initToken, indexToken));
    unequalPairs.add(new Pair<>(initToken, journalToken));
    unequalPairs.add(new Pair<>(initToken, inclusiveJournalToken));
    unequalPairs.add(new Pair<>(indexToken, journalToken));
    unequalPairs.add(new Pair<>(indexToken, inclusiveJournalToken));
    unequalPairs.add(new Pair<>(indexToken,
        new StoreFindToken(key, otherOffset, sessionId, incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION)));
    unequalPairs.add(new Pair<>(indexToken,
        new StoreFindToken(otherKey, offset, sessionId, incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION)));
    unequalPairs.add(new Pair<>(journalToken,
        new StoreFindToken(otherOffset, sessionId, incarnationId, false, null, null, UNINITIALIZED_RESET_KEY_VERSION)));
    unequalPairs.add(new Pair<>(inclusiveJournalToken, journalToken));
    unequalPairs.add(new Pair<>(indexToken,
        new StoreFindToken(key, offset, sessionId, incarnationId, resetKey, resetKeyType,
            UNINITIALIZED_RESET_KEY_VERSION)));

    for (Pair<StoreFindToken, StoreFindToken> unequalPair : unequalPairs) {
      StoreFindToken first = unequalPair.getFirst();
      StoreFindToken second = unequalPair.getSecond();
      assertFalse("StoreFindTokens [" + first + "] and [" + second + "] should not be equal",
          unequalPair.getFirst().equals(unequalPair.getSecond()));
    }
  }

  /**
   * Tests {@link StoreFindToken} serialization/deserialization.
   * @throws IOException
   */
  @Test
  public void serDeTest() throws IOException {
    UUID sessionId = UUID.randomUUID();
    UUID incarnationId = UUID.randomUUID();
    LogSegmentName logSegmentName = LogSegmentName.generateFirstSegmentName(isLogSegmented);
    Offset offset = new Offset(logSegmentName, 0);
    MockId key = new MockId(TestUtils.getRandomString(10));
    MockId resetKey = new MockId(TestUtils.getRandomString(10));
    PersistentIndex.IndexEntryType resetKeyType =
        PersistentIndex.IndexEntryType.values()[(new Random()).nextInt(PersistentIndex.IndexEntryType.values().length)];
    short resetKeyVersion = (short) random.nextInt(5);
    if (!isLogSegmented) {
      // UnInitialized
      doSerDeTest(new StoreFindToken(), VERSION_0, VERSION_1, VERSION_2, VERSION_3);

      // Journal based token
      doSerDeTest(
          new StoreFindToken(offset, sessionId, incarnationId, false, null, null, UNINITIALIZED_RESET_KEY_VERSION),
          VERSION_0, VERSION_1, VERSION_2, VERSION_3);
      // Journal based token with resetKey and resetKeyType specified (VERSION_3)
      doSerDeTest(new StoreFindToken(offset, sessionId, incarnationId, false, resetKey, resetKeyType, resetKeyVersion),
          VERSION_3);

      // inclusiveness is present only in {VERSION_2, VERSION_3}
      doSerDeTest(
          new StoreFindToken(offset, sessionId, incarnationId, true, null, null, UNINITIALIZED_RESET_KEY_VERSION),
          VERSION_2, VERSION_3);
      doSerDeTest(new StoreFindToken(offset, sessionId, incarnationId, true, resetKey, resetKeyType, resetKeyVersion),
          VERSION_3);

      // Index based
      doSerDeTest(
          new StoreFindToken(key, offset, sessionId, incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION),
          VERSION_0, VERSION_1, VERSION_2, VERSION_3);
      doSerDeTest(new StoreFindToken(key, offset, sessionId, incarnationId, resetKey, resetKeyType, resetKeyVersion),
          VERSION_3);
    } else {
      // UnInitialized
      doSerDeTest(new StoreFindToken(), VERSION_1, VERSION_2, VERSION_3);

      // Journal based token
      doSerDeTest(
          new StoreFindToken(offset, sessionId, incarnationId, false, null, null, UNINITIALIZED_RESET_KEY_VERSION),
          VERSION_1, VERSION_2, VERSION_3);
      doSerDeTest(new StoreFindToken(offset, sessionId, incarnationId, false, resetKey, resetKeyType, resetKeyVersion),
          VERSION_3);

      // inclusiveness is present only in VERSION_2
      doSerDeTest(
          new StoreFindToken(offset, sessionId, incarnationId, true, null, null, UNINITIALIZED_RESET_KEY_VERSION),
          VERSION_2, VERSION_3);
      doSerDeTest(new StoreFindToken(offset, sessionId, incarnationId, true, resetKey, resetKeyType, resetKeyVersion),
          VERSION_3);

      // Index based
      doSerDeTest(
          new StoreFindToken(key, offset, sessionId, incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION),
          VERSION_1, VERSION_2, VERSION_3);
      doSerDeTest(new StoreFindToken(key, offset, sessionId, incarnationId, resetKey, resetKeyType, resetKeyVersion),
          VERSION_3);
    }
  }

  /**
   * Tests {@link StoreFindToken} for construction error cases.
   */
  @Test
  public void constructionErrorCasesTest() {
    UUID sessionId = UUID.randomUUID();
    UUID incarnationId = UUID.randomUUID();
    LogSegmentName logSegmentName = LogSegmentName.generateFirstSegmentName(isLogSegmented);
    Offset offset = new Offset(logSegmentName, 0);
    MockId key = new MockId(TestUtils.getRandomString(10));
    MockId resetKey = new MockId(TestUtils.getRandomString(10));
    PersistentIndex.IndexEntryType resetKeyType =
        PersistentIndex.IndexEntryType.values()[(new Random()).nextInt(PersistentIndex.IndexEntryType.values().length)];
    short resetKeyVersion = (short) random.nextInt(5);

    // no offset
    testConstructionFailure(key, sessionId, incarnationId, null);
    // no session id
    testConstructionFailure(key, null, incarnationId, offset);
    // no incarnation Id
    testConstructionFailure(key, sessionId, null, offset);

    // no key in IndexBased
    try {
      new StoreFindToken(null, offset, sessionId, null, null, null, UNINITIALIZED_RESET_KEY_VERSION);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // version 3 token without reset key or reset key type
    for (FindTokenType type : EnumSet.of(FindTokenType.JournalBased, FindTokenType.IndexBased)) {
      for (Pair<MockId, PersistentIndex.IndexEntryType> pair : Arrays.asList(
          new Pair<MockId, PersistentIndex.IndexEntryType>(resetKey, null),
          new Pair<MockId, PersistentIndex.IndexEntryType>(null, resetKeyType))) {
        try {
          new StoreFindToken(type, offset, key, sessionId, incarnationId, type == FindTokenType.JournalBased, VERSION_3,
              pair.getFirst(), pair.getSecond(), resetKeyVersion);
          fail("Construction of StoreFindToken should have failed because rest key or its type is null.");
        } catch (IllegalArgumentException e) {
          // expected
        }
      }
    }
  }

  // helpers
  // general

  /**
   * Compares two tokens to ensure their equality test passes and that their hash codes match.
   * @param reference the reference {@link StoreFindToken}
   * @param toCheck the {@link StoreFindToken} to check
   */
  private void compareTokens(StoreFindToken reference, StoreFindToken toCheck) {
    assertEquals("Tokens do not match", reference, toCheck);
    assertEquals("Hash code does not match", reference.hashCode(), toCheck.hashCode());
  }

  // serDeTest() helpers

  /**
   * Serializes {@code token} in all formats and ensures that the {@link StoreFindToken} obtained from the
   * deserialization matches the original.
   * @param token the {@link StoreFindToken} that has to be serialized/deserialized.
   * @param versions {@link List} of valid versions that the token to be tested for
   * @throws IOException
   */
  private void doSerDeTest(StoreFindToken token, Short... versions) throws IOException {
    for (Short version : versions) {
      DataInputStream stream = getSerializedStream(token, version);
      StoreFindToken deSerToken = StoreFindToken.fromBytes(stream, STORE_KEY_FACTORY);
      assertEquals("Stream should have ended ", 0, stream.available());
      assertEquals("Version mismatch for token ", version.shortValue(), deSerToken.getVersion());
      compareTokens(token, deSerToken);
      assertEquals("SessionId does not match", token.getSessionId(), deSerToken.getSessionId());
      if (version >= VERSION_2) {
        assertEquals("IncarnationId mismatch ", token.getIncarnationId(), deSerToken.getIncarnationId());
      }
      if (version == VERSION_3) {
        assertEquals("Reset key mismatch ", token.getResetKey(), deSerToken.getResetKey());
        assertEquals("Reset key type mismatch", token.getResetKeyType(), deSerToken.getResetKeyType());
        assertEquals("Reset key life version mismatch", token.getResetKeyVersion(), deSerToken.getResetKeyVersion());
      }
      // use StoreFindToken's actual serialize method to verify that token is serialized in the expected
      // version
      stream = new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(deSerToken.toBytes())));
      deSerToken = StoreFindToken.fromBytes(stream, STORE_KEY_FACTORY);
      assertEquals("Stream should have ended ", 0, stream.available());
      assertEquals("Version mismatch for token ", version.shortValue(), deSerToken.getVersion());
      compareTokens(token, deSerToken);
      assertEquals("SessionId does not match", token.getSessionId(), deSerToken.getSessionId());
      if (version >= VERSION_2) {
        assertEquals("IncarnationId mismatch ", token.getIncarnationId(), deSerToken.getIncarnationId());
      }
    }
  }

  /**
   * Gets a serialized format of {@code token} in the version {@code version}.
   * @param token the {@link StoreFindToken} to serialize.
   * @param version the version to serialize it in.
   * @return a serialized format of {@code token} in the version {@code version}.
   */
  static DataInputStream getSerializedStream(StoreFindToken token, short version) {
    byte[] bytes;
    FindTokenType type = token.getType();
    byte[] sessionIdBytes = token.getSessionIdInBytes();
    byte[] storeKeyInBytes = token.getStoreKeyInBytes();
    switch (version) {
      case StoreFindToken.VERSION_0:
        // version size + sessionId length size + session id size + log offset size + index segment start offset size
        // + store key size
        int size = 2 + 4 + sessionIdBytes.length + 8 + 8 + storeKeyInBytes.length;
        bytes = new byte[size];
        ByteBuffer bufWrap = ByteBuffer.wrap(bytes);
        // add version
        bufWrap.putShort(StoreFindToken.VERSION_0);
        // add sessionId
        bufWrap.putInt(sessionIdBytes.length);
        bufWrap.put(sessionIdBytes);
        long logOffset = -1;
        long indexStartOffset = -1;
        if (type.equals(FindTokenType.JournalBased)) {
          logOffset = token.getOffset().getOffset();
        } else if (type.equals(FindTokenType.IndexBased)) {
          indexStartOffset = token.getOffset().getOffset();
        }
        // add offset
        bufWrap.putLong(logOffset);
        // add index start offset
        bufWrap.putLong(indexStartOffset);
        // add storeKey
        if (storeKeyInBytes.length > 0) {
          bufWrap.put(storeKeyInBytes);
        }
        break;
      case StoreFindToken.VERSION_1:
        byte[] offsetBytes = token.getOffsetInBytes();
        // version size + sessionId length size + session id size + type + log offset / index segment start offset size
        // + store key size
        size = 2 + 4 + sessionIdBytes.length + 2 + offsetBytes.length + storeKeyInBytes.length;
        bytes = new byte[size];
        bufWrap = ByteBuffer.wrap(bytes);
        // add version
        bufWrap.putShort(StoreFindToken.VERSION_1);
        // add sessionId
        bufWrap.putInt(sessionIdBytes.length);
        bufWrap.put(sessionIdBytes);
        // add type
        bufWrap.putShort((byte) type.ordinal());
        bufWrap.put(offsetBytes);
        if (storeKeyInBytes.length > 0) {
          bufWrap.put(storeKeyInBytes);
        }
        break;
      case VERSION_2:
        offsetBytes = token.getOffsetInBytes();
        byte[] incarnationIdBytes = token.getIncarnationIdInBytes();
        size = VERSION_SIZE + TYPE_SIZE;
        if (type != FindTokenType.Uninitialized) {
          size +=
              INCARNATION_ID_LENGTH_SIZE + incarnationIdBytes.length + SESSION_ID_LENGTH_SIZE + sessionIdBytes.length
                  + offsetBytes.length;
          if (type == FindTokenType.JournalBased) {
            size += INCLUSIVE_BYTE_SIZE;
          } else if (type == FindTokenType.IndexBased) {
            size += storeKeyInBytes.length;
          }
        }
        bytes = new byte[size];
        bufWrap = ByteBuffer.wrap(bytes);
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
            bufWrap.put(token.getInclusive() ? (byte) 1 : (byte) 0);
          } else if (type == FindTokenType.IndexBased) {
            bufWrap.put(storeKeyInBytes);
          }
        }
        break;
      case StoreFindToken.CURRENT_VERSION:
        bytes = token.toBytes();
        break;
      default:
        throw new IllegalArgumentException("Version " + version + " of StoreFindToken does not exist");
    }
    return new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(bytes)));
  }

  // construction failure tests

  /**
   * Verify that construction of {@link StoreFindToken} fails
   * @param key the {@link StoreKey} to be used for constructing the {@link StoreFindToken}
   * @param sessionId the sessionId to be used for constructing the {@link StoreFindToken}
   * @param incarnationId the incarnationId to be used for constructing the {@link StoreFindToken}
   * @param offset the {@link Offset} to be used for constructing the {@link StoreFindToken}
   */
  private void testConstructionFailure(StoreKey key, UUID sessionId, UUID incarnationId, Offset offset) {
    // journal based
    try {
      new StoreFindToken(offset, sessionId, incarnationId, true, null, null, UNINITIALIZED_RESET_KEY_VERSION);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // index based
    try {
      new StoreFindToken(key, offset, sessionId, incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  // UUID generation helpers

  /**
   * Generate random {@link UUID} different from {@code oldUUID}
   * @param oldUUID the new {@link UUID} generated that should be different from this {@link UUID}
   * @return the newly generated random {@link UUID}
   */
  private UUID getRandomUUID(UUID oldUUID) {
    UUID newUUID = oldUUID;
    while (newUUID == oldUUID) {
      newUUID = UUID.randomUUID();
    }
    return newUUID;
  }
}
