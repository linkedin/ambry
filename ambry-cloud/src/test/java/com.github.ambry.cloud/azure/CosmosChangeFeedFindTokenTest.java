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
package com.github.ambry.cloud.azure;

import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for {@link CosmosChangeFeedFindToken}
 */
public class CosmosChangeFeedFindTokenTest {

  /**
   * Test for correctness of {@code CosmosChangeFeedFindToken#equals(Object)}
   */
  @Test
  public void equalityTest() {
    short version = 0;
    Random random = new Random();
    long bytesRead = random.nextLong();
    String startContinuationToken = "start";
    String endContinuationToken = "end";
    int totalItems = random.nextInt();
    int index = random.nextInt() % totalItems;
    String azureRequestId = UUID.randomUUID().toString();

    //compare empty tokens
    ensureEqual(new CosmosChangeFeedFindToken(), new CosmosChangeFeedFindToken());

    //compare token constructed from all constructors
    CosmosChangeFeedFindToken token1 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
            azureRequestId);
    CosmosChangeFeedFindToken token2 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
            azureRequestId);
    ensureEqual(token1, token2);

    token1 = new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
        azureRequestId, version);
    token2 = new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
        azureRequestId, version);
    ensureEqual(token1, token2);

    //ensure inequality for any unequal field
    token2 =
        new CosmosChangeFeedFindToken(bytesRead + 1, startContinuationToken, endContinuationToken, index, totalItems,
            azureRequestId, version);
    ensureUnequal(token1, token2);

    token2 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken + "1", endContinuationToken, index, totalItems,
            azureRequestId);
    ensureUnequal(token1, token2);

    token2 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken + "1", index, totalItems,
            azureRequestId);
    ensureUnequal(token1, token2);

    token2 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index + 1, totalItems,
            azureRequestId);
    ensureUnequal(token1, token2);

    token2 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems + 1,
            azureRequestId);
    ensureUnequal(token1, token2);

    token2 = new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
        UUID.randomUUID().toString());
    ensureUnequal(token1, token2);

    token2 = new CosmosChangeFeedFindToken();
    ensureUnequal(token1, token2);
  }

  /**
   * Test for serialization and deserialization of cloud token
   * @throws IOException if an IO exception happens during deserialization
   */
  @Test
  public void serdeTest() throws IOException {
    Random random = new Random();
    long bytesRead = random.nextLong();
    String startContinuationToken = "start";
    String endContinuationToken = "end";
    int totalItems = random.nextInt();
    int index = random.nextInt() % totalItems;
    String azureRequestId = UUID.randomUUID().toString();

    //Deserialization test

    //token with invalid version
    CosmosChangeFeedFindToken invalidToken =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
            azureRequestId, (short) 1);
    DataInputStream tokenStream = getSerializedStream(invalidToken);
    try {
      CosmosChangeFeedFindToken.fromBytes(tokenStream);
      fail("deserialization of token with invalid version should have failed");
    } catch (IllegalStateException ise) {
    }

    //valid token
    CosmosChangeFeedFindToken token =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
            azureRequestId);
    tokenStream = new DataInputStream(new ByteArrayInputStream(token.toBytes()));
    CosmosChangeFeedFindToken deSerToken = CosmosChangeFeedFindToken.fromBytes(tokenStream);
    assertEquals("Stream should have ended ", 0, tokenStream.available());
    assertEquals(token, deSerToken);

    //Serialization test

    //token with invalid version
    DataInputStream serializedStream = getSerializedStream(invalidToken);
    try {
      CosmosChangeFeedFindToken.fromBytes(serializedStream);
      fail("serialization of token with invalid version should have failed");
    } catch (IllegalStateException ise) {
    }

    //valid token
    serializedStream = new DataInputStream(new ByteArrayInputStream(token.toBytes()));
    deSerToken = CosmosChangeFeedFindToken.fromBytes(serializedStream);
    assertEquals("Stream should have ended ", 0, serializedStream.available());
    assertEquals(token, deSerToken);
  }

  /**
   * helper to ensure that token passed are equal
   * @param token1 token to be checked.
   * @param token2 token to be checked.
   */
  private void ensureEqual(CosmosChangeFeedFindToken token1, CosmosChangeFeedFindToken token2) {
    assertEquals("Tokens should match", token1, token2);
    assertEquals("Hashcode of tokens should match", token1.hashCode(), token2.hashCode());
  }

  /**
   * helper to ensure that token passed are not equal
   * @param token1 token to be checked.
   * @param token2 token to be checked.
   */
  private void ensureUnequal(CosmosChangeFeedFindToken token1, CosmosChangeFeedFindToken token2) {
    assertFalse("Tokens shouldn't match", token1.equals(token2));
  }

  /**
   * helper to serialize token.
   * @param token {@code AzureFindToken} object to serialize
   * @return DataInputStream serialized stream
   */
  private DataInputStream getSerializedStream(CosmosChangeFeedFindToken token) {
    byte[] buf = new byte[token.size()];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    bufWrap.putShort(token.getVersion());
    bufWrap.putShort((short) token.getType().ordinal());
    bufWrap.putLong(token.getBytesRead());
    Utils.serializeNullableString(bufWrap, token.getStartContinuationToken());
    Utils.serializeNullableString(bufWrap, token.getEndContinuationToken());
    bufWrap.putInt(token.getIndex());
    bufWrap.putInt(token.getTotalItems());
    Utils.serializeNullableString(bufWrap, token.getCacheSessionId());
    return new DataInputStream(new ByteArrayInputStream(buf));
  }
}
