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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for {@link CosmosUpdateTimeFindTokenFactory}
 */
public class CosmosUpdateTimeFindTokenFactoryTest {

  /**
   * test get find token from stream
   * @throws java.io.IOException if an IO exception happens during deserialization
   */
  @Test
  public void getFindTokenTest() throws IOException {
    short version = 0;
    Random random = new Random();
    long latestBlobUpdateTime = random.nextLong();
    long bytesRead = random.nextLong();
    Set<String> lastReadBlobIds = new HashSet<>();
    lastReadBlobIds.add("blobid1");
    lastReadBlobIds.add("blobid2");

    CosmosUpdateTimeFindToken cosmosUpdateTimeFindToken1 =
        new CosmosUpdateTimeFindToken(version, latestBlobUpdateTime, bytesRead, lastReadBlobIds);
    DataInputStream stream = new DataInputStream(new ByteArrayInputStream(cosmosUpdateTimeFindToken1.toBytes()));
    CosmosUpdateTimeFindToken cosmosUpdateTimeFindToken2 =
        (CosmosUpdateTimeFindToken) new CosmosUpdateTimeFindTokenFactory().getFindToken(stream);
    assertEquals("incorrect token returned from factory", cosmosUpdateTimeFindToken1, cosmosUpdateTimeFindToken2);
  }

  /**
   * test get new find token
   */
  @Test
  public void getNewFindTokenTest() {
    CosmosUpdateTimeFindToken cosmosUpdateTimeFindToken1 =
        (CosmosUpdateTimeFindToken) new CosmosUpdateTimeFindTokenFactory().getNewFindToken();
    CosmosUpdateTimeFindToken cosmosUpdateTimeFindToken2 = new CosmosUpdateTimeFindToken();
    assertEquals("tokens should be equal", cosmosUpdateTimeFindToken1, cosmosUpdateTimeFindToken2);
  }
}
