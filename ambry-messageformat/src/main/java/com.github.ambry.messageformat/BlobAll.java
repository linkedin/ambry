/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;


/**
 * Contains the store key, blob properties, user metadata, and data associated with a blob.
 */
public class BlobAll {
  private final StoreKey storeKey;
  private final BlobInfo blobInfo;
  private final BlobData blobData;

  /**
   * Construct an object containing the store key, blob properties, user metadata, and data for a blob.
   * @param blobInfo the {@link BlobInfo} for this blob.
   * @param blobData the {@link BlobData} for this blob.
   */
  public BlobAll(StoreKey storeKey, BlobInfo blobInfo, BlobData blobData) {
    this.storeKey = storeKey;
    this.blobInfo = blobInfo;
    this.blobData = blobData;
  }

  /**
   * @return the {@link StoreKey} for this blob.
   */
  public StoreKey getStoreKey() {
    return storeKey;
  }

  /**
   * @return the {@link BlobInfo} for this blob.
   */
  public BlobInfo getBlobInfo() {
    return blobInfo;
  }

  /**
   * @return the {@link BlobData} for this blob.
   */
  public BlobData getBlobData() {
    return blobData;
  }
}
