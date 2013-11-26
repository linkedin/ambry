package com.github.ambry.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

abstract class BloomFilterSerializer
{
  public void serialize(BloomFilter bf, DataOutput out) throws IOException
  {
    out.writeInt(bf.hashCount);
    bf.bitset.serialize(out);
  }

  public BloomFilter deserialize(DataInput in) throws IOException
  {
    int hashes = in.readInt();
    IBitSet bs = OpenBitSet.deserialize(in);
    return createFilter(hashes, bs);
  }

  protected abstract BloomFilter createFilter(int hashes, IBitSet bs);
}
