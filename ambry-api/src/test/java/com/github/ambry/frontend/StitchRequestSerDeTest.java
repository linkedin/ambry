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
 */

package com.github.ambry.frontend;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link StitchRequestSerDe}.
 */
public class StitchRequestSerDeTest {

  /**
   * Test {@link StitchRequestSerDe}'s serialization/deserialization methods.
   * @throws Exception
   */
  @Test
  public void serDeTest() {
    doSerDeTest(null);
    doSerDeTest(Collections.emptyList());
    doSerDeTest(Arrays.asList("a", "b", "/signedId/abcdef"));
  }

  /**
   * Perform a test of serialization/deserialization with specific inputs.
   * @param signedChunkIds the list of signed chunk IDs to test with.
   */
  private void doSerDeTest(List<String> signedChunkIds) {
    JSONObject stitchRequestJson = StitchRequestSerDe.toJson(signedChunkIds);
    assertEquals("Unexpected deserialized signed chunk IDs",
        signedChunkIds == null ? Collections.emptyList() : signedChunkIds,
        StitchRequestSerDe.fromJson(stitchRequestJson));
  }
}
