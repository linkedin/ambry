/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.protocol;

import com.github.ambry.utils.Crc32;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;


/**
 * The Crc32 implementaion interface.
 */
public interface Crc32Impl {

  /**
   * Update Crc32 value based on given {@link ByteBuffer}.
   * @param buffer The {@link ByteBuffer} to update crc32 value.
   */
  void update(ByteBuffer buffer);

  /**
   * Return crc32 value.
   * @return the final crc32 value.jek
   */
  long getValue();

  /**
   * Return java native implementation of crc32 algorithm.
   * @return
   */
  static Crc32Impl getJavaNativeInstance() {
    return new Crc32Impl() {
      final private CRC32 crc32 = new CRC32();

      @Override
      public void update(ByteBuffer buffer) {
        crc32.update(buffer);
      }

      @Override
      public long getValue() {
        return crc32.getValue();
      }
    };
  }

  /**
   * Return ambry util implementation of crc32 algorithm.
   * @return
   */
  static Crc32Impl getAmbryInstance() {
    return new Crc32Impl() {
      final private Crc32 crc32 = new Crc32();

      @Override
      public void update(ByteBuffer buffer) {
        crc32.update(buffer);
      }

      @Override
      public long getValue() {
        return crc32.getValue();
      }
    };
  }
}
