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
import com.fasterxml.jackson.databind.ObjectMapper;
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

    jsonAsString = "{\"v\":100,\"first\":{\"v\":40},\"second\":{\"v\":60}}";

    snapshot = new ObjectMapper().readValue(jsonAsString, StatsSnapshot.class);

    assertEquals("Mismatch in total aggregated value for StatsSnapshot", 100L, snapshot.getValue());
    assertEquals("Mismatch in aggregated value for first account", 40L, snapshot.getSubMap().get("first").getValue());
    assertEquals("Mismatch in aggregated value for second account", 60L, snapshot.getSubMap().get("second").getValue());
    assertEquals("The subMap in first account should be null", null, snapshot.getSubMap().get("first").getSubMap());
    assertEquals("The subMap in second account should be null", null, snapshot.getSubMap().get("second").getSubMap());
  }

  @Test
  public void testCopyConstructor() {
    // Anonymous class with static block initialization, not a good way to create map, it's only for testing.
    StatsSnapshot original = new StatsSnapshot(60L, new HashMap<String, StatsSnapshot>() {
      {
        put("L1_K1", new StatsSnapshot(30L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K1", new StatsSnapshot(10L, null));
            put("L2_K2", new StatsSnapshot(20L, null));
          }
        }));
        put("L1_K2", new StatsSnapshot(30L, null));
      }
    });
    StatsSnapshot copy = new StatsSnapshot(original);
    assertNotSame(copy, original);
    assertEquals(original, copy);

    assertNotSame(copy.getSubMap(), original.getSubMap());
    assertEquals(original.getSubMap(), copy.getSubMap());
  }

  /**
   * Test {@link StatsSnapshot#updateValue()}.
   */
  @Test
  public void testUpdateValue() {
    // Anonymous class with static block initialization, not a good way to create map, it's only for testing.
    StatsSnapshot snapshot = new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>() {
      {
        put("L1_K1", new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K1", new StatsSnapshot(40L, null));
            put("L2_K2", new StatsSnapshot(60L, null));
          }
        }));
        put("L1_K2", new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K3", new StatsSnapshot(70L, null));
            put("L2_K4", new StatsSnapshot(90L, null));
          }
        }));
        put("L1_K3", new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K5", new StatsSnapshot(20L, new HashMap<String, StatsSnapshot>() {
              {
                put("L3_K1", new StatsSnapshot(100L, null));
                put("L3_K2", new StatsSnapshot(200L, null));
              }
            }));
            put("L2_K6", new StatsSnapshot(300L, null));
          }
        }));
      }
    });
    snapshot.updateValue();
    assertEquals(40 + 60 + 70 + 90 + 100 + 200 + 300, snapshot.getValue());
    assertEquals(40 + 60, snapshot.getSubMap().get("L1_K1").getValue());
    assertEquals(70 + 90, snapshot.getSubMap().get("L1_K2").getValue());
    assertEquals(100 + 200 + 300, snapshot.getSubMap().get("L1_K3").getValue());
    assertEquals(100 + 200, snapshot.getSubMap().get("L1_K3").getSubMap().get("L2_K5").getValue());
  }

  /**
   * Test {@link StatsSnapshot#removeZeroValueSnapshots()}
   */
  @Test
  public void testRemoveZeroValueSnapshots() {
    // Anonymous class with static block initialization, not a good way to create map, it's only for testing.
    StatsSnapshot snapshot = new StatsSnapshot(370L, new HashMap<String, StatsSnapshot>() {
      {
        put("L1_K1", new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K1", new StatsSnapshot(0L, null));
            put("L2_K2", new StatsSnapshot(0L, null));
          }
        }));
        put("L1_K2", new StatsSnapshot(70L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K3", new StatsSnapshot(70L, null));
            put("L2_K4", new StatsSnapshot(0L, null));
          }
        }));
        put("L1_K3", new StatsSnapshot(300L, new HashMap<String, StatsSnapshot>() {
          {
            put("L2_K5", new StatsSnapshot(0L, new HashMap<String, StatsSnapshot>() {
              {
                put("L3_K1", new StatsSnapshot(0L, null));
                put("L3_K2", new StatsSnapshot(0L, null));
              }
            }));
            put("L2_K6", new StatsSnapshot(300L, null));
          }
        }));
      }
    });
    snapshot.removeZeroValueSnapshots();
    /* it will be trimmed to :
       {
         "value": 370,
         "L1_K2": {
           "value": 70,
           "L2_K3": { "value": 70 }
         },
         "L1_K3": {
           "value": 300,
           "L2_K6": { "value": 300 }
         }
       }
     */
    assertEquals(2, snapshot.getSubMap().size());
    assertTrue(snapshot.getSubMap().containsKey("L1_K2"));
    assertTrue(snapshot.getSubMap().containsKey("L1_K3"));
    assertFalse(snapshot.getSubMap().containsKey("L1_K1"));

    assertEquals(70, snapshot.getSubMap().get("L1_K2").getValue());
    assertEquals(1, snapshot.getSubMap().get("L1_K2").getSubMap().size());
    assertTrue(snapshot.getSubMap().get("L1_K2").getSubMap().containsKey("L2_K3"));
    assertFalse(snapshot.getSubMap().get("L1_K2").getSubMap().containsKey("L2_K4"));
    assertEquals(70, snapshot.getSubMap().get("L1_K2").getSubMap().get("L2_K3").getValue());
    assertNull(snapshot.getSubMap().get("L1_K2").getSubMap().get("L2_K3").getSubMap());

    assertEquals(300, snapshot.getSubMap().get("L1_K3").getValue());
    assertEquals(1, snapshot.getSubMap().get("L1_K3").getSubMap().size());
    assertTrue(snapshot.getSubMap().get("L1_K3").getSubMap().containsKey("L2_K6"));
    assertFalse(snapshot.getSubMap().get("L1_K3").getSubMap().containsKey("L2_K5"));
    assertEquals(300, snapshot.getSubMap().get("L1_K3").getSubMap().get("L2_K6").getValue());
    assertNull(snapshot.getSubMap().get("L1_K3").getSubMap().get("L2_K6").getSubMap());
  }
}
