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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for {@link CloudFindToken}
 */
public class CloudFindTokenTest {

  /**
   * Test for correctness of {@code CloudFindToken#equals(Object)}
   */
  @Test
  public void equalityTest() {
    short version = 0;
    Random random = new Random();
    long lastBlobUpdateTime = random.nextLong();
    long bytesRead = random.nextLong();
    Set<String> lastReadBlobIds = new HashSet<>();
    lastReadBlobIds.add("blobid1");
    lastReadBlobIds.add("blobid2");

    //compare empty tokens
    ensureEqual(new CloudFindToken(), new CloudFindToken());

    //compare token constructed from all constructors
    CloudFindToken token1 = new CloudFindToken(lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    CloudFindToken token2 = new CloudFindToken(lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    ensureEqual(token1, token2);

    token1 = new CloudFindToken(version, lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    token2 = new CloudFindToken(version, lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    ensureEqual(token1, token2);

    //ensure inequality for any unequal field
    token2 = new CloudFindToken((short) 1, lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken(version, lastBlobUpdateTime + 100, bytesRead, lastReadBlobIds);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken(version, lastBlobUpdateTime, bytesRead, new HashSet<>());
    ensureUnequal(token1, token2);

    Set<String> unEqualBlobidSet = new HashSet<>();
    unEqualBlobidSet.add("blobid1");
    token2 = new CloudFindToken(version, lastBlobUpdateTime, bytesRead, unEqualBlobidSet);
    ensureUnequal(token1, token2);

    unEqualBlobidSet.add("blobid3");
    token2 = new CloudFindToken(version, lastBlobUpdateTime, bytesRead, unEqualBlobidSet);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken(version, lastBlobUpdateTime, bytesRead + 10, lastReadBlobIds);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken();
    ensureUnequal(token1, token2);
  }

  /**
   * Test for serialization and deserialization of cloud token
   * @throws IOException if an IO exception happens during deserialization
   */
  @Test
  public void serdeTest() throws IOException {
    short version = 0;
    Random random = new Random();
    long lastBlobUpdateTime = random.nextLong();
    long bytesRead = random.nextLong();
    Set<String> lastReadBlobIds = new HashSet<>();
    lastReadBlobIds.add("blobid1");
    lastReadBlobIds.add("blobid2");

    //Deserialization test

    //token with invalid version
    CloudFindToken invalidToken = new CloudFindToken((short) 1, lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    DataInputStream tokenStream = getSerializedStream(invalidToken);
    try {
      CloudFindToken.fromBytes(tokenStream);
      fail("deserialization of token with invalid version should have failed");
    } catch (IllegalStateException ise) {
    }

    //valid token
    CloudFindToken token = new CloudFindToken(version, lastBlobUpdateTime, bytesRead, lastReadBlobIds);
    tokenStream = getSerializedStream(token);
    CloudFindToken deSerToken = CloudFindToken.fromBytes(tokenStream);
    assertEquals("Stream should have ended ", 0, tokenStream.available());
    assertEquals(token, deSerToken);

    //Serialization test

    //token with invalid version
    DataInputStream serializedStream = getSerializedStream(invalidToken);
    try {
      CloudFindToken.fromBytes(serializedStream);
      fail("serialization of token with invalid version should have failed");
    } catch (IllegalStateException ise) {
    }

    //valid token
    serializedStream = new DataInputStream(new ByteArrayInputStream(token.toBytes()));
    deSerToken = CloudFindToken.fromBytes(serializedStream);
    assertEquals("Stream should have ended ", 0, serializedStream.available());
    assertEquals(token, deSerToken);
  }

  /**
   * helper to ensure that token passed are equal
   * @param token1
   * @param token2
   */
  private void ensureEqual(CloudFindToken token1, CloudFindToken token2) {
    assertEquals("Tokens should match", token1, token2);
    assertEquals("Hashcode of tokens should match", token1.hashCode(), token2.hashCode());
  }

  /**
   * helper to ensure that token passed are not equal
   * @param token1
   * @param token2
   */
  private void ensureUnequal(CloudFindToken token1, CloudFindToken token2) {
    assertFalse("Tokens shouldn't match", token1.equals(token2));
  }

  /**
   * helper to seriliaze token.
   * @param token {@code CloudFindToken} object to serialize
   * @return DataInputStream serialized stream
   */
  private DataInputStream getSerializedStream(CloudFindToken token) {
    int size = 2 * Short.BYTES + 2 * Long.BYTES + Short.BYTES;
    for (String blobId : token.getLastUpdateTimeReadBlobIds()) {
      size += Short.BYTES; //for size of string
      size += blobId.length(); //for the string itself
    }
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort(token.getVersion());
    // add type
    bufWrap.putShort((short) token.getType().ordinal());
    // add latestUploadTime
    bufWrap.putLong(token.getLastUpdateTime());
    // add bytesRead
    bufWrap.putLong(token.getBytesRead());
    // add lastUpdateTimeReadBlobIds
    bufWrap.putShort((short) token.getLastUpdateTimeReadBlobIds().size());
    for (String blobId : token.getLastUpdateTimeReadBlobIds()) {
      bufWrap.putShort((short) blobId.length());
      bufWrap.put(blobId.getBytes());
    }
    return new DataInputStream(new ByteArrayInputStream(buf));
  }
}
