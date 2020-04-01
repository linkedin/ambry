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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


abstract class BloomFilterSerializer {
  public void serialize(BloomFilter bf, DataOutput out) throws IOException {
    out.writeInt(bf.hashCount);
    bf.bitset.serialize(out);
  }

  public BloomFilter deserialize(DataInput in) throws IOException {
    int hashes = in.readInt();
    IBitSet bs = OpenBitSet.deserialize(in);
    return createFilter(hashes, bs);
  }

  protected abstract BloomFilter createFilter(int hashes, IBitSet bs);
}
