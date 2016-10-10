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
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Tests for {@link Offset}.
 */
public class OffsetTest {

  /**
   * Tests serialization and deserialization of an {@link Offset} class.
   * @throws IOException
   */
  @Test
  public void offsetSerDeTest()
      throws IOException {
    String name = UtilsTest.getRandomString(10);
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
  public void offsetBadInputTest()
      throws IOException {
    doBadOffsetInputTest(null, 10);
    doBadOffsetInputTest("", 10);
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
    // TODO (Log Segmentation): Improve and add more cases once compareTo also uses the name.
    Offset lower = new Offset("lower", 1);
    Offset match = new Offset("match", 1);
    Offset higher = new Offset("higher", 2);
    assertEquals("CompareTo result is inconsistent", -1, lower.compareTo(higher));
    assertEquals("CompareTo result is inconsistent", 0, lower.compareTo(match));
    assertEquals("CompareTo result is inconsistent", 1, higher.compareTo(lower));
  }

  /**
   * Tests for {@link Offset#equals(Object)} and {@link Offset#hashCode()}.
   */
  @Test
  public void equalsAndHashCodeTest() {
    Offset o1 = new Offset("test", 2);
    Offset o2 = new Offset("test", 2);
    Offset o3 = new Offset("test", 3);
    Offset o4 = new Offset("test_more", 2);

    assertTrue("Offset should be equal to itself", o1.equals(o1));
    assertFalse("Offset should not be equal to null", o1.equals(null));
    assertFalse("Offset should not be equal to random Object", o1.equals(new Object()));

    assertTrue("Offsets should be declared equal", o1.equals(o2));
    assertEquals("Hashcode mismatch", o1.hashCode(), o2.hashCode());

    assertFalse("Offsets should be declared unequal", o1.equals(o3));

    // TODO (Log Segmentation): Uncomment when compareTo() can handle different offset names.
    /*
    assertFalse("Offsets should be declared unequal", o1.equals(o4));
    */
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
