/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

/**
 * Values for different ways in which the index of a {@link Store} can be maintained. The values are hints for the
 * actual implementations which can choose to respect or ignore the hints.
 */
public enum IndexMemState {
  /**
   * Index should be read from an mmap-ed file, but not forced to reside in memory.
   */
  MMAP_WITHOUT_FORCE_LOAD,

  /**
   * Index should be mmap-ed and force loaded into memory. The index should make a best effort to keep the segments in
   * memory, but it is not guaranteed.
   */
  MMAP_WITH_FORCE_LOAD,

  /**
   * Index should be in heap memory.
   */
  IN_HEAP_MEM,

  /**
   * Index should be in direct (off-heap) memory.
   */
  IN_DIRECT_MEM
}
