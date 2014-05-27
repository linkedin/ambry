package com.github.ambry.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FilterFactory {

  private static final Logger logger = LoggerFactory.getLogger(FilterFactory.class);
  private static final long BITSET_EXCESS = 20;

  public static void serialize(IFilter bf, DataOutput output)
      throws IOException {
    Murmur3BloomFilter.serializer.serialize((Murmur3BloomFilter) bf, output);
  }

  public static IFilter deserialize(DataInput input)
      throws IOException {
    return Murmur3BloomFilter.serializer.deserialize(input);
  }

  /**
   * @return A BloomFilter with the lowest practical false positive
   *         probability for the given number of elements.
   */
  public static IFilter getFilter(long numElements, int targetBucketsPerElem) {
    int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
    int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
    if (bucketsPerElement < targetBucketsPerElem) {
      logger.warn(String
          .format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).", numElements,
              bucketsPerElement, targetBucketsPerElem));
    }
    BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
    return createFilter(spec.K, numElements, spec.bucketsPerElement);
  }

  /**
   * @return The smallest BloomFilter that can provide the given false
   *         positive probability rate for the given number of elements.
   *
   *         Asserts that the given probability can be satisfied using this
   *         filter.
   */
  public static IFilter getFilter(long numElements, double maxFalsePosProbability) {
    int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
    BloomCalculations.BloomSpecification spec =
        BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
    return createFilter(spec.K, numElements, spec.bucketsPerElement);
  }

  private static IFilter createFilter(int hash, long numElements, int bucketsPer) {
    long numBits = (numElements * bucketsPer) + BITSET_EXCESS;
    IBitSet bitset = new OpenBitSet(numBits);
    return new Murmur3BloomFilter(hash, bitset);
  }
}
