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

package com.github.ambry.router;

public class AmbryCacheStats {

  final int numCacheHit;
  final int numCacheMiss;
  final int numPut;
  final int numDelete;

  public AmbryCacheStats(int numCacheHit, int numCacheMiss, int numPut, int numDelete) {
    this.numCacheHit = numCacheHit;
    this.numCacheMiss = numCacheMiss;
    this.numPut = numPut;
    this.numDelete = numDelete;
  }

  public int getNumCacheHit() {
    return numCacheHit;
  }

  public int getNumCacheMiss() {
    return numCacheMiss;
  }

  public int getNumPut() {
    return numPut;
  }

  public int getNumDelete() {
    return numDelete;
  }

}
