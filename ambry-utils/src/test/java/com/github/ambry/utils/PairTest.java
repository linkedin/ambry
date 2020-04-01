/*
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

package com.github.ambry.utils;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test the {@link Pair} utility class.
 */
public class PairTest {
  /**
   * Test the {@link Pair} class for correctness.
   */
  @Test
  public void testPair() {
    String first = "abcdef";
    Long second = 23L;

    Pair<String, Long> pairOne = new Pair<>(first, second);
    Pair<String, Long> pairTwo = new Pair<>(first, second);
    Pair<String, Long> pairThree = new Pair<>(first, second + 1);
    Pair<String, Long> pairFour = new Pair<>(first + "extra", second);
    Pair<Long, String> pairFive = new Pair<>(second, first);

    assertEquals("Invalid first item.", first, pairOne.getFirst());
    assertEquals("Invalid second item.", second, pairOne.getSecond());
    assertEquals("These pairs should be equivalent", pairTwo, pairOne);
    assertFalse("pairOne and pairThree should not be equal", pairOne.equals(pairThree));
    assertFalse("pairOne and pairFour should not be equal", pairOne.equals(pairFour));
    assertFalse("pairOne and pairFive should not be equal", pairOne.equals(pairFive));
    assertEquals("Hashcodes should be the same", pairOne.hashCode(), pairTwo.hashCode());
    assertEquals("Pair{first=" + first + ", second=" + second + "}", pairOne.toString());
  }
}
