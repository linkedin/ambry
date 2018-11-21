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
   * Index should not be in memory
   */
  NOT_IN_MEM,

  /**
   * Index should be in heap memory
   */
  IN_HEAP_MEM,

  /**
   * If mmaped, the index should be force loaded into memory
   */
  FORCE_LOAD_MMAP
}
