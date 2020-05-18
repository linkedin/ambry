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
 * Interface to implement a listener that will be called on updates to {@link DataNodeConfig}s.
 */
public interface DataNodeConfigChangeListener {
  /**
   * Called when there is a relevant update to one or more {@link DataNodeConfig}s.
   * @param configs the new or updated configs.
   */
  void onDataNodeConfigChange(Iterable<DataNodeConfig> configs);
}
