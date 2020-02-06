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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for {@link com.github.ambry.cloud.azure.CosmosChangeFeedFindTokenFactory}
 */
public class CosmosChangeFeedFindTokenFactoryTest {

  /**
   * test get find token from stream
   * @throws IOException if an IO exception happens during deserialization
   */
  @Test
  public void getFindTokenTest() throws IOException {
    short version = 0;
    Random random = new Random();
    long bytesRead = random.nextLong();
    String startContinuationToken = "start";
    String endContinuationToken = "end";
    int totalItems = random.nextInt();
    int index = random.nextInt() % totalItems;
    String azureRequestId = UUID.randomUUID().toString();

    CosmosChangeFeedFindToken cosmosChangeFeedFindToken1 =
        new CosmosChangeFeedFindToken(bytesRead, startContinuationToken, endContinuationToken, index, totalItems,
            azureRequestId, version);
    DataInputStream stream = new DataInputStream(new ByteArrayInputStream(cosmosChangeFeedFindToken1.toBytes()));
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken2 =
        (CosmosChangeFeedFindToken) new CosmosChangeFeedFindTokenFactory().getFindToken(stream);
    assertEquals("incorrect token returned from factory", cosmosChangeFeedFindToken1, cosmosChangeFeedFindToken2);
  }

  /**
   * test get new find token
   */
  @Test
  public void getNewFindTokenTest() {
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken1 =
        (CosmosChangeFeedFindToken) new CosmosChangeFeedFindTokenFactory().getNewFindToken();
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken2 = new CosmosChangeFeedFindToken();
    assertEquals("tokens should be equal", cosmosChangeFeedFindToken1, cosmosChangeFeedFindToken2);
  }
}
