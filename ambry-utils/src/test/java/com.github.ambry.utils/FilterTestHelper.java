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
package com.github.ambry.utils;

import java.nio.ByteBuffer;


public class FilterTestHelper {
  // used by filter subclass tests

  static final double MAX_FAILURE_RATE = 0.1;
  public static final BloomCalculations.BloomSpecification spec =
      BloomCalculations.computeBloomSpec(15, MAX_FAILURE_RATE);
  static final int ELEMENTS = 10000;

  static final ResetableIterator<ByteBuffer> intKeys() {
    return new KeyGenerator.IntGenerator(ELEMENTS);
  }

  static final ResetableIterator<ByteBuffer> randomKeys() {
    return new KeyGenerator.RandomStringGenerator(314159, ELEMENTS);
  }

  static final ResetableIterator<ByteBuffer> randomKeys2() {
    return new KeyGenerator.RandomStringGenerator(271828, ELEMENTS);
  }

  public static double testFalsePositives(IFilter f, ResetableIterator<ByteBuffer> keys,
      ResetableIterator<ByteBuffer> otherkeys) {
    assert keys.size() == otherkeys.size();

    while (keys.hasNext()) {
      f.add(keys.next());
    }

    int fp = 0;
    while (otherkeys.hasNext()) {
      if (f.isPresent(otherkeys.next())) {
        fp++;
      }
    }

    double fp_ratio = fp / (keys.size() * BloomCalculations.probs[spec.bucketsPerElement][spec.K]);
    assert fp_ratio < 1.03 : fp_ratio;
    return fp_ratio;
  }

  public void testTrue() {
    assert true;
  }
}
