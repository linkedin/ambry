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
 * A source of {@link DataNodeConfig}s for a cluster. Can be used for config change notification and administration.
 */
interface DataNodeConfigSource {
  /**
   * Attach a listener that will be notified when there are new or updated {@link DataNodeConfig}s.
   * @param listener the {@link DataNodeConfigChangeListener} to attach.
   */
  void addServerConfigChangeListener(DataNodeConfigChangeListener listener) throws Exception;
}

