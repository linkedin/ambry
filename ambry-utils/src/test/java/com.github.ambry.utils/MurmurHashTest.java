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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test the murmur hash
 */
public class MurmurHashTest {
  @Test
  public void testCrcOutputStream() throws IOException {
    byte[] buf = new byte[1024];
    int hash1 = MurmurHash.hash32(ByteBuffer.wrap(buf), 0, 1024, 10);
    int hash2 = MurmurHash.hash32(ByteBuffer.wrap(buf), 0, 1024, 10);
    Assert.assertEquals(hash1, hash2);
    long hash3 = MurmurHash.hash2_64(ByteBuffer.wrap(buf), 0, 1024, 10);
    long hash4 = MurmurHash.hash2_64(ByteBuffer.wrap(buf), 0, 1024, 10);
    Assert.assertEquals(hash3, hash4);
    long[] hashes1 = MurmurHash.hash3_x64_128(ByteBuffer.wrap(buf), 0, 1024, 10);
    long[] hashes2 = MurmurHash.hash3_x64_128(ByteBuffer.wrap(buf), 0, 1024, 10);
    Assert.assertEquals(hashes1.length, hashes2.length);
    for (int i = 0; i < hashes1.length; i++) {
      Assert.assertEquals(hashes1[i], hashes2[i]);
    }
  }
}
