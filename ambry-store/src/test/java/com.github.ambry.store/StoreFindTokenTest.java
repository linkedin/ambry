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

import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link StoreFindToken}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class StoreFindTokenTest {
  private static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final long numSegments;

  /**
   * Running for both segmented and non-segmented log.
   * @return an array that returns 1 and 2 (non segmented and segmented)
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{1L}, {2L}});
  }

  public StoreFindTokenTest(long numSegments) {
    this.numSegments = numSegments;
  }

  /**
   * Tests the correctness of {@link StoreFindToken#equals(Object)}.
   */
  @Test
  public void equalityTest() {
    UUID sessionId = new UUID(0, 0);
    String logSegmentName = LogSegmentNameHelper.generateFirstSegmentName(numSegments);
    Offset offset = new Offset(logSegmentName, 0);
    Offset otherOffset = new Offset(logSegmentName, 1);
    MockId key = new MockId(UtilsTest.getRandomString(10));
    MockId otherKey = new MockId(UtilsTest.getRandomString(10));

    StoreFindToken initToken = new StoreFindToken();
    StoreFindToken otherInitToken = new StoreFindToken();
    StoreFindToken indexToken = new StoreFindToken(key, offset, sessionId);
    StoreFindToken otherIndexToken = new StoreFindToken(key, offset, sessionId);
    StoreFindToken journalToken = new StoreFindToken(offset, sessionId);
    StoreFindToken otherJournalToken = new StoreFindToken(offset, sessionId);

    // equality
    compareTokens(initToken, initToken);
    compareTokens(initToken, otherInitToken);
    compareTokens(indexToken, indexToken);
    compareTokens(indexToken, otherIndexToken);
    compareTokens(journalToken, journalToken);
    compareTokens(journalToken, otherJournalToken);

    // equality even if session IDs are different
    compareTokens(indexToken, new StoreFindToken(key, offset, sessionId));
    compareTokens(journalToken, new StoreFindToken(offset, new UUID(1, 1)));

    // inequality if some fields differ
    List<Pair<StoreFindToken, StoreFindToken>> unequalPairs = new ArrayList<>();
    unequalPairs.add(new Pair<>(initToken, indexToken));
    unequalPairs.add(new Pair<>(initToken, journalToken));
    unequalPairs.add(new Pair<>(indexToken, journalToken));
    unequalPairs.add(new Pair<>(indexToken, new StoreFindToken(key, otherOffset, sessionId)));
    unequalPairs.add(new Pair<>(indexToken, new StoreFindToken(otherKey, offset, sessionId)));
    unequalPairs.add(new Pair<>(journalToken, new StoreFindToken(otherOffset, sessionId)));

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
    UUID sessionId = new UUID(0, 0);
    String logSegmentName = LogSegmentNameHelper.generateFirstSegmentName(numSegments);
    Offset offset = new Offset(logSegmentName, 0);
    MockId key = new MockId(UtilsTest.getRandomString(10));

    doSerDeTest(new StoreFindToken());
    doSerDeTest(new StoreFindToken(key, offset, sessionId));
    doSerDeTest(new StoreFindToken(offset, sessionId));
  }

  /**
   * Tests {@link StoreFindToken} for construction error cases.
   */
  @Test
  public void constructionErrorCasesTest() {
    UUID sessionId = new UUID(0, 0);
    String logSegmentName = LogSegmentNameHelper.generateFirstSegmentName(numSegments);
    Offset offset = new Offset(logSegmentName, 0);
    MockId key = new MockId(UtilsTest.getRandomString(10));

    // no offset in JournalBased
    try {
      new StoreFindToken(null, sessionId);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // no sessionId in JournalBased
    try {
      new StoreFindToken(offset, null);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // no key in IndexBased
    try {
      new StoreFindToken(null, offset, sessionId);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // no offset in IndexBased
    try {
      new StoreFindToken(key, null, sessionId);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // no sessionId in IndexBased
    try {
      new StoreFindToken(key, offset, null);
      fail("Construction of StoreFindToken should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
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
   * @throws IOException
   */
  private void doSerDeTest(StoreFindToken token) throws IOException {
    short i = numSegments > 1 ? (short) 1 : (short) 0;
    for (; i <= 1; i++) {
      DataInputStream stream = getSerializedStream(token, i);
      StoreFindToken deSerToken = StoreFindToken.fromBytes(stream, STORE_KEY_FACTORY);
      compareTokens(token, deSerToken);
      assertEquals("SessionId does not match", token.getSessionId(), deSerToken.getSessionId());
    }
  }

  /**
   * Gets a serialized format of {@code token} in the version {@code version}.
   * @param token the {@link StoreFindToken} to serialize.
   * @param version the version to serialize it in.
   * @return a serialized format of {@code token} in the version {@code version}.
   */
  private DataInputStream getSerializedStream(StoreFindToken token, short version) {
    byte[] bytes;
    switch (version) {
      case StoreFindToken.VERSION_0:
        UUID sessionId = token.getSessionId();
        StoreKey key = token.getStoreKey();
        // version size + sessionId length size + session id size + log offset size + index segment start offset size
        // + store key size
        int size = 2 + 4 + (sessionId == null ? 0 : sessionId.toString().getBytes().length) + 8 + 8 + (key == null ? 0
            : key.sizeInBytes());
        bytes = new byte[size];
        ByteBuffer bufWrap = ByteBuffer.wrap(bytes);
        // add version
        bufWrap.putShort(StoreFindToken.VERSION_0);
        // add sessionId
        byte[] sessionIdBytes = sessionId == null ? null : sessionId.toString().getBytes();
        bufWrap.putInt(sessionIdBytes == null ? 0 : sessionIdBytes.length);
        if (sessionIdBytes != null) {
          bufWrap.put(sessionIdBytes);
        }
        long logOffset = -1;
        long indexStartOffset = -1;
        StoreFindToken.Type type = token.getType();
        if (type.equals(StoreFindToken.Type.JournalBased)) {
          logOffset = token.getOffset().getOffset();
        } else if (type.equals(StoreFindToken.Type.IndexBased)) {
          indexStartOffset = token.getOffset().getOffset();
        }
        // add offset
        bufWrap.putLong(logOffset);
        // add index start offset
        bufWrap.putLong(indexStartOffset);
        // add storekey
        if (key != null) {
          bufWrap.put(key.toBytes());
        }
        break;
      case StoreFindToken.VERSION_1:
        bytes = token.toBytes();
        break;
      default:
        throw new IllegalArgumentException("Version " + version + " of StoreFindToken does not exist");
    }
    return new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(bytes)));
  }
}
