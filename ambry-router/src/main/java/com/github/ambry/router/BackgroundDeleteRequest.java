/*
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

package com.github.ambry.router;

import com.github.ambry.store.StoreKey;


/**
 * This class contains the request parameters for a background delete operation.
 */
class BackgroundDeleteRequest {
  static final String SERVICE_ID_PREFIX = "ambry-background-delete-";
  private final StoreKey storeKey;
  private final String serviceId;

  /**
   * @param storeKey The {@link StoreKey} to delete.
   * @param serviceIdSuffix The suffix to attach to the delete service ID. This can be used to convey information about
   *                        the the delete requester.
   */
  BackgroundDeleteRequest(StoreKey storeKey, String serviceIdSuffix) {
    this.serviceId = SERVICE_ID_PREFIX + serviceIdSuffix;
    this.storeKey = storeKey;
  }

  /**
   * @return The blob ID string to delete.
   */
  public String getBlobId() {
    return storeKey.getID();
  }

  /**
   * @return the service ID to use for the deletion
   */
  public String getServiceId() {
    return serviceId;
  }
}
