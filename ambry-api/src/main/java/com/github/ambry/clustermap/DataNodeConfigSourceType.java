/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.clustermap;

/**
 * Represent the type of {@code DataNodeConfigSource} implementation to use.
 */
public enum DataNodeConfigSourceType {
  INSTANCE_CONFIG(true, false),
  PROPERTY_STORE(false, true),
  COMPOSITE_INSTANCE_CONFIG_PRIMARY(true, true),
  COMPOSITE_PROPERTY_STORE_PRIMARY(true, true);

  private final boolean instanceConfigAware;
  private final boolean propertyStoreAware;

  /**
   * @param instanceConfigAware {@code true} if this type depends on helix instance configs.
   * @param propertyStoreAware {@code true} if this type depends on the helix property store.
   */
  DataNodeConfigSourceType(boolean instanceConfigAware, boolean propertyStoreAware) {
    this.instanceConfigAware = instanceConfigAware;
    this.propertyStoreAware = propertyStoreAware;
  }

  /**
   * @return {@code true} if this type depends on helix instance configs.
   */
  public boolean isInstanceConfigAware() {
    return instanceConfigAware;
  }

  /**
   * @return {@code true} if this type depends on the helix property store.
   */
  public boolean isPropertyStoreAware() {
    return propertyStoreAware;
  }
}
