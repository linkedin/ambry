/**
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

import com.github.ambry.commons.BlobId;


class StoreKeyStringCompare {

  private StoreKey storeKey;

  StoreKeyStringCompare(StoreKey storeKey) {
    this.storeKey = storeKey;
  }

  public StoreKey getStoreKey() {
    return storeKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StoreKeyStringCompare that = (StoreKeyStringCompare) o;

    return storeKey != null ? storeKey.getID().equals(that.storeKey.getID()) : that.storeKey == null;
  }

  @Override
  public int hashCode() {
    return storeKey != null ? storeKey.getID().hashCode() : 0;
  }

  @Override
  public String toString() {
    return storeKey.toString();
  }
}
