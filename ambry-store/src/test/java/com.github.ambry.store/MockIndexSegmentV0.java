/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;


/**
 * Mock {@link IndexSegment} that uses version {@link PersistentIndex#VERSION_0}
 */
class MockIndexSegmentV0 extends IndexSegment {

  MockIndexSegmentV0(String dataDir, Offset startOffset, StoreKeyFactory factory, int keySize, int valueSize,
      StoreConfig config, StoreMetrics metrics, Time time) {
    super(dataDir, startOffset, factory, keySize, valueSize, config, metrics, time);
  }

  @Override
  Pair<StoreKey, PersistentIndex.IndexEntryType> getResetKey() {
    return null;
  }

  @Override
  short getVersion() {
    return PersistentIndex.VERSION_0;
  }
}
