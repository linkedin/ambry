/**
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

package com.github.ambry.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;


/**
 * Serialization/deserialization unit tests of {@link StatsSnapshot}.
 */
public class StatsSnapshotTest {

  /**
   * Serialization unit test of {@link StatsSnapshot}.
   * Test if subMap and null fields are removed from serialized result.
   */
  @Test
  public void serializeStatsSnapshotTest() throws IOException {
    Long val = 100L;
    Map<String, StatsSnapshot> subMap = new HashMap<>();
    subMap.put("first", new StatsSnapshot(40L, null));
    subMap.put("second", new StatsSnapshot(60L, null));
    StatsSnapshot snapshot = new StatsSnapshot(val, subMap);

    String result = new ObjectMapper().writeValueAsString(snapshot);

    assertThat("Result should contain \"first\" keyword and associated entry", result, containsString("first"));
    assertThat("Result should contain \"second\" keyword and associated entry", result, containsString("second"));
    assertThat("Result should not contain \"subMap\" keyword", result, not(containsString("subMap")));
    assertThat("Result should ignore any null fields", result, not(containsString("null")));
  }

  /**
   * Deserialization unit test of {@link StatsSnapshot}.
   * Test if {@link StatsSnapshot} can be reconstructed from json string in the form of a directory or tree (consist of
   * value and subMap fields).
   */
  @Test
  public void deserializeStatsSnapshotTest() throws IOException {
    String jsonAsString = "{\"value\":100,\"first\":{\"value\":40},\"second\":{\"value\":60}}";

    StatsSnapshot snapshot = new ObjectMapper().readValue(jsonAsString, StatsSnapshot.class);

    assertEquals("Mismatch in total aggregated value for StatsSnapshot", 100L, snapshot.getValue());
    assertEquals("Mismatch in aggregated value for first account", 40L, snapshot.getSubMap().get("first").getValue());
    assertEquals("Mismatch in aggregated value for second account", 60L, snapshot.getSubMap().get("second").getValue());
    assertEquals("The subMap in first account should be null", null, snapshot.getSubMap().get("first").getSubMap());
    assertEquals("The subMap in second account should be null", null, snapshot.getSubMap().get("second").getSubMap());
  }
}
