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

import com.github.ambry.replication.FindTokenType;
import com.github.ambry.utils.UtilsTest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
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
    FindTokenType findTokenType = FindTokenType.CloudBased;
    Random random = new Random();
    long latestBlobUploadTime = random.nextLong();
    String latestBlobId = UtilsTest.getRandomString(10);
    long bytesRead = random.nextLong();

    //compare empty tokens
    ensureEqual(new CloudFindToken(), new CloudFindToken());

    //compare token constructed from all constructors
    CloudFindToken token1 = new CloudFindToken(latestBlobUploadTime, latestBlobId, bytesRead);
    CloudFindToken token2 = new CloudFindToken(latestBlobUploadTime, latestBlobId, bytesRead);
    ensureEqual(token1, token2);

    token1 = new CloudFindToken(version, latestBlobUploadTime, latestBlobId, bytesRead);
    token2 = new CloudFindToken(version, latestBlobUploadTime, latestBlobId, bytesRead);
    ensureEqual(token1, token2);

    //ensure inequality for any unequal field
    token2 = new CloudFindToken((short) 1, latestBlobUploadTime, latestBlobId, bytesRead);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken(version, latestBlobUploadTime + 100, latestBlobId, bytesRead);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken(version, latestBlobUploadTime, "", bytesRead);
    ensureUnequal(token1, token2);

    token2 = new CloudFindToken(version, latestBlobUploadTime, latestBlobId, bytesRead + 10);
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
    FindTokenType findTokenType = FindTokenType.CloudBased;
    Random random = new Random();
    long latestBlobUploadTime = random.nextLong();
    String latestBlobId = UtilsTest.getRandomString(10);
    long bytesRead = random.nextLong();

    //Deserialization test

    //token with invalid version
    CloudFindToken invalidToken = new CloudFindToken((short) 1, latestBlobUploadTime, latestBlobId, bytesRead);
    DataInputStream tokenStream = getSerializedStream(invalidToken);
    try {
      CloudFindToken deSerToken = CloudFindToken.fromBytes(tokenStream);
      fail("deserialization of token with invalid version should have failed");
    } catch (IllegalStateException ise) {
    }

    //valid token
    CloudFindToken token = new CloudFindToken(version, latestBlobUploadTime, latestBlobId, bytesRead);
    tokenStream = getSerializedStream(token);
    CloudFindToken deSerToken = CloudFindToken.fromBytes(tokenStream);
    assertEquals("Stream should have ended ", 0, tokenStream.available());
    assertEquals(token, deSerToken);

    //Serialization test

    //token with invalid version
    DataInputStream serializedStream = getSerializedStream(invalidToken);
    try {
      deSerToken = CloudFindToken.fromBytes(serializedStream);
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
    assertFalse("Tokens should match", token1.equals(token2));
    assertFalse("Tokens should match", token1.hashCode() == token2.hashCode());
  }

  /**
   * helper to seriliaze token.
   * @param token {@code CloudFindToken} object to serialize
   * @return DataInputStream serialized stream
   */
  private DataInputStream getSerializedStream(CloudFindToken token) {
    int size = 3 * Short.BYTES + 2 * Long.BYTES;
    if (token.getLatestBlobId() != null) {
      size += token.getLatestBlobId().length();
    }
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort(token.getVersion());
    // add type
    bufWrap.putShort((short) token.getType().ordinal());
    // add latestUploadTime
    bufWrap.putLong(token.getLatestUploadTime());
    // add bytesRead
    bufWrap.putLong(token.getBytesRead());
    if (token.getLatestBlobId() != null) {
      bufWrap.putShort((short) token.getLatestBlobId().length());
      bufWrap.put(token.getLatestBlobId().getBytes());
    } else {
      bufWrap.putShort((short) 0);
    }
    return new DataInputStream(new ByteArrayInputStream(buf));
  }
}
