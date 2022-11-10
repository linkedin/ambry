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
 * Base compression with level support.
 * It adds shared/common level implementation to the base compression class.
 * Subclasses are required to implement default, minimum, and maximum level.
 */
public abstract class BaseCompressionWithLevel extends BaseCompression implements CompressionLevel {

  /**
   * The current compressor level.
   */
  private Integer compressionLevel;

  /**
   * Get the current compression level, set by setCompressionLevel().
   * If never set, it returns the default compression level.
   * @return The current compression level.
   */
  @Override
  public int getCompressionLevel() {
    return compressionLevel != null ? compressionLevel : getDefaultCompressionLevel();
  }

  /**
   * Set the current compression level.  The new compression level must be between minimum and maximum level range.
   * @param newCompressionLevel The new compression level.
   */
  @Override
  public void setCompressionLevel(int newCompressionLevel) {
    if (newCompressionLevel < getMinimumCompressionLevel() || newCompressionLevel > getMaximumCompressionLevel()) {
      throw new IllegalArgumentException(
          "Invalid new compression level " + newCompressionLevel + ".  Level must be between "
              + getMinimumCompressionLevel() + " and " + getMaximumCompressionLevel());
    }

    compressionLevel = newCompressionLevel;
  }
}
