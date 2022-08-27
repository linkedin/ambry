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

/**
 * Not all compression algorithm supports compression levels.
 * For compression algorithms that do support levels, implement this interface.
 * If an algorithm does not implement this interface, it implies it does not support compression levels.
 *
 * This interface contains these properties:
 * - the desired compression level.
 * - the default compression level.
 * - range of compression level.
 */
public interface CompressionLevel {
  /**
   * Get the minimum compression level.
   * @return The minimum compression level.
   */
  int getMinimumCompressionLevel();

  /**
   * Get the maximum compression level.
   * @return The maximum compression level.
   */
  int getMaximumCompressionLevel();

  /**
   * Get the default compression level if not set.
   * @return The default compression level.
   */
  int getDefaultCompressionLevel();

  /**
   * Get the current compression level, set by setCompressionLevel().
   * If never set, it returns the default compression level.
   * @return The current compression level.
   */
  int getCompressionLevel();

  /**
   * Set the current compression level.  The new compression level must be between minimum and maximum level range.
   * @param newCompressionLevel The new compression level.
   */
  void setCompressionLevel(int newCompressionLevel);
}
