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

package com.github.ambry.router;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link PutBlobOptions}.
 */
public class PutBlobOptionsTest {
  /**
   * Test that the chunk upload and max size options can be assigned and retrieved correctly.
   * @throws Exception
   */
  @Test
  public void testOptions() {
    PutBlobOptions options = new PutBlobOptionsBuilder().chunkUpload(true).build();
    assertTrue("chunkUpload from options not as expected.", options.isChunkUpload());
    assertEquals("maxUploadSize from options not as expected.", Long.MAX_VALUE, options.getMaxUploadSize());
    options = new PutBlobOptionsBuilder().chunkUpload(false).maxUploadSize(3).build();
    assertFalse("chunkUpload from options not as expected.", options.isChunkUpload());
    assertEquals("maxUploadSize from options not as expected.", 3, options.getMaxUploadSize());
  }

  /**
   * Test toString, equals, and hashCode methods.
   */
  @Test
  public void testToStringEqualsAndHashcode() {
    PutBlobOptions a = new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(3).build();
    PutBlobOptions b = new PutBlobOptionsBuilder().chunkUpload(true).maxUploadSize(3).build();
    assertEquals("PutBlobOptions should be equal", a, b);
    assertEquals("PutBlobOptions hashcodes should be equal", a.hashCode(), b.hashCode());
    assertEquals("toString output not as expected", "PutBlobOptions{chunkUpload=true, maxUploadSize=3}", a.toString());
    b = new PutBlobOptionsBuilder().chunkUpload(false).maxUploadSize(3).build();
    assertThat("PutBlobOptions should not be equal.", a, not(b));
  }
}
