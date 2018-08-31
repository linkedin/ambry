/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.frontend;

import com.github.ambry.utils.Pair;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link SignedIdSerDe}.
 */
public class SignedIdSerDeTest {

  /**
   * Test {@link SignedIdSerDe}'s serialization/deserialization methods.
   * @throws Exception
   */
  @Test
  public void serDeTest() throws Exception {
    doSerDeTest("blob-id", Collections.emptyMap());
    doSerDeTest("blob-id", Collections.singletonMap("key-one", "value-one"));
    doSerDeTest("blob id with spaces", Collections.singletonMap("key-one", "value-one"));
    doSerDeTest("blob-id",
        Stream.of("a", "b", "c", "d with spaces").collect(Collectors.toMap(s -> s + "key", s -> s + "value")));
  }

  /**
   * Perform a test of serialization/deserialization with specific inputs.
   * @param blobId the blob ID to test with.
   * @param metadata the metadata to test with.
   * @throws Exception
   */
  private void doSerDeTest(String blobId, Map<String, String> metadata) throws Exception {
    String jsonString = SignedIdSerDe.toJson(blobId, metadata);
    // ensure that the string is valid json.
    new JSONObject(jsonString);
    assertEquals("Unexpected deserialized ID and metadata value", new Pair<>(blobId, metadata),
        SignedIdSerDe.fromJson(jsonString));
  }
}
