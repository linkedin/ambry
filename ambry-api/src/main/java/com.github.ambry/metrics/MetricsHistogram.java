package com.github.ambry.metrics;

/**
 * A metrics histogram interface
 */
public interface MetricsHistogram {
  /**
   * Updates the histogram with the given value
   * @param value The value that the histogram needs to be updated with
   */
  void update(long value);

  /**
   * Updates the histogram with the given value
   * @param value The value that the histogram needs to be updated with
   */
  void update(int value);
}
