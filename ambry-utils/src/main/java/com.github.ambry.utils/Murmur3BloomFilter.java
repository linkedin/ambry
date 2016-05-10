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


public class Murmur3BloomFilter extends BloomFilter {
  public static final Murmur3BloomFilterSerializer serializer = new Murmur3BloomFilterSerializer();

  public Murmur3BloomFilter(int hashes, IBitSet bs) {
    super(hashes, bs);
  }

  protected long[] hash(ByteBuffer b, int position, int remaining, long seed) {
    return MurmurHash.hash3_x64_128(b, b.position(), b.remaining(), seed);
  }

  public static class Murmur3BloomFilterSerializer extends BloomFilterSerializer {
    protected BloomFilter createFilter(int hashes, IBitSet bs) {
      return new Murmur3BloomFilter(hashes, bs);
    }
  }
}