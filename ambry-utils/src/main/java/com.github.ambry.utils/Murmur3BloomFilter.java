package com.github.ambry.utils;

import java.nio.ByteBuffer;


public class Murmur3BloomFilter extends BloomFilter
{
  public static final Murmur3BloomFilterSerializer serializer = new Murmur3BloomFilterSerializer();

  public Murmur3BloomFilter(int hashes, IBitSet bs)
  {
    super(hashes, bs);
  }

  protected long[] hash(ByteBuffer b, int position, int remaining, long seed)
  {
    return MurmurHash.hash3_x64_128(b, b.position(), b.remaining(), seed);
  }

  public static class Murmur3BloomFilterSerializer extends BloomFilterSerializer
  {
    protected BloomFilter createFilter(int hashes, IBitSet bs)
    {
      return new Murmur3BloomFilter(hashes, bs);
    }
  }
}