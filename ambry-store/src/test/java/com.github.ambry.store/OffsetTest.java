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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link Offset}.
 */
public class OffsetTest {

  /**
   * Tests serialization and deserialization of an {@link Offset} class.
   * @throws IOException
   */
  @Test
  public void offsetSerDeTest() throws IOException {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String name = LogSegmentNameHelper.getName(pos, gen);
    long offset = Utils.getRandomLong(new Random(), Long.MAX_VALUE);
    Offset logOffset = new Offset(name, offset);
    byte[] serialized = logOffset.toBytes();
    Offset deserializedOffset =
        Offset.fromBytes(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(serialized))));
    assertEquals("Original offset 'name' does not match with the deserialized offset", name,
        deserializedOffset.getName());
    assertEquals("Original offset 'offset' does not match with the deserialized offset", offset,
        deserializedOffset.getOffset());
    // equals test
    assertEquals("Original offset does not match with the deserialized offset", logOffset, deserializedOffset);
    // hashcode test
    assertEquals("Hashcode doesn't match", logOffset.hashCode(), deserializedOffset.hashCode());
  }

  /**
   * Tests the constructor and {@link Offset#fromBytes(DataInputStream)} function with bad input.
   * @throws IOException
   */
  @Test
  public void offsetBadInputTest() throws IOException {
    doBadOffsetInputTest(null, 10);
    doBadOffsetInputTest("1_11_log", -1);

    Offset offset = new Offset("1_11_log", 10);
    byte[] serialized = offset.toBytes();
    // mess with a version byte
    serialized[0] = serialized[0] == (byte) 1 ? (byte) 2 : (byte) 1;
    try {
      Offset.fromBytes(new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(serialized))));
      fail("Version check should have failed");
    } catch (IllegalArgumentException e) {
      // expected.
    }
  }

  /**
   * Tests {@link Offset#compareTo(Offset)}.
   */
  @Test
  public void compareToTest() {
    List<Offset> offsets = new ArrayList<>();
    offsets.add(new Offset("0_0", 0));
    offsets.add(new Offset("0_0", 1));
    offsets.add(new Offset("0_1", 0));
    offsets.add(new Offset("0_1", 1));
    offsets.add(new Offset("1_0", 0));
    offsets.add(new Offset("1_0", 1));
    offsets.add(new Offset("2_0", 0));
    offsets.add(new Offset("3_0", 1));
    offsets.add(new Offset("10_0", 0));
    offsets.add(new Offset("21_0", 0));
    for (int i = 0; i < offsets.size(); i++) {
      for (int j = 0; j < offsets.size(); j++) {
        int expectCompare = i == j ? 0 : i > j ? 1 : -1;
        assertEquals("Unexpected value on compare", expectCompare, offsets.get(i).compareTo(offsets.get(j)));
        assertEquals("Unexpected value on compare", -1 * expectCompare, offsets.get(j).compareTo(offsets.get(i)));
      }
    }
  }

  /**
   * Tests for {@link Offset#equals(Object)} and {@link Offset#hashCode()}.
   */
  @Test
  public void equalsAndHashCodeTest() {
    Offset o1 = new Offset("1_1", 2);
    Offset o2 = new Offset("1_1", 2);
    Offset o3 = new Offset("1_1", 3);
    Offset o4 = new Offset("1_2", 2);

    assertTrue("Offset should be equal to itself", o1.equals(o1));
    assertFalse("Offset should not be equal to null", o1.equals(null));
    assertFalse("Offset should not be equal to random Object", o1.equals(new Object()));

    assertTrue("Offsets should be declared equal", o1.equals(o2));
    assertEquals("Hashcode mismatch", o1.hashCode(), o2.hashCode());

    assertFalse("Offsets should be declared unequal", o1.equals(o3));
    assertFalse("Offsets should be declared unequal", o1.equals(o4));
  }

  // helpers
  // offsetBadInputTest()

  /**
   * Tests constructor of {@link Offset} with bad input.
   * @param name the name to use.
   * @param offset the offset to use.
   */
  private void doBadOffsetInputTest(String name, long offset) {
    try {
      new Offset(name, offset);
      fail("Should have thrown because one of the inputs is invalid");
    } catch (IllegalArgumentException e) {
      // expected.
    }
  }
}
