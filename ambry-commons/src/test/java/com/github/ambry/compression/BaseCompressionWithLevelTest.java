/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.compression;

import com.github.ambry.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class BaseCompressionWithLevelTest {

  @Test
  public void testGetDefaultCompressionLevel() {
    BaseCompressionWithLevel compressor = Mockito.mock(BaseCompressionWithLevel.class, Mockito.CALLS_REAL_METHODS);
    Mockito.when(compressor.getDefaultCompressionLevel()).thenReturn(10);

    int level = compressor.getCompressionLevel();
    Assert.assertEquals(10, level);
  }

  @Test
  public void testSetAndGetCompressionLevel() {
    BaseCompressionWithLevel compressor = Mockito.mock(BaseCompressionWithLevel.class, Mockito.CALLS_REAL_METHODS);
    Mockito.when(compressor.getMinimumCompressionLevel()).thenReturn(0);
    Mockito.when(compressor.getMaximumCompressionLevel()).thenReturn(10);

    // Test: Out of range.
    Throwable ex = TestUtils.getException(() -> compressor.setCompressionLevel(20));
    Assert.assertTrue(ex instanceof IllegalArgumentException);

    // Test: Valid range.
    compressor.setCompressionLevel(2);
    int level = compressor.getCompressionLevel();
    Assert.assertEquals(2, level);
  }
}
