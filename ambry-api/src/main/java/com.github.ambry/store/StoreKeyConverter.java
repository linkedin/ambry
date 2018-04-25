package com.github.ambry.store;

import java.util.Collection;
import java.util.Map;


/**
 * This is a service that can be used to convert store keys across different formats.
 * </p>
 * Typical usage will be to map b/w different formats of keys that refer to the same blob. The input/output
 * formats are expected to be set when the converter is constructed.
 */
public interface StoreKeyConverter {

  /**
   * Returns a mapping for every {@link StoreKey} in {@code input}. If a {@link StoreKey} is already in the format
   * desired, returns the same {@link StoreKey} as the mapping. If no mapping exists, returns {@code null} as the
   * mapping.
   * (Note: TBD if we should return {@code null} if already in format requested but it is useful to diffrentiate
   * b/w the cases)
   * @param input the {@link StoreKey}s that need to be converted.
   * @return a mapping for each {@link StoreKey} in the new format.
   * @throws Exception that may be thrown when performing the conversion operation
   */
  public Map<StoreKey, StoreKey> convert(Collection<StoreKey> input) throws Exception;
}