/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;


/**
 * {@link ClusterSpectator} adds itself as a spectator of a cluster and registers some cluster change listeners.
 */
public interface ClusterSpectator extends InstanceConfigChangeListener, LiveInstanceChangeListener {

  /**
   * Register with helix cluster as a spectator.
   */
  void spectate() throws Exception;

  /**
   * Register a listener for changes to instance config in cluster.
   * @param instanceConfigChangeListener listener to add.
   */
  void registerInstanceConfigChangeListener(InstanceConfigChangeListener instanceConfigChangeListener);

  /**
   * Register a listener for changes to instance liveness information in cluster.
   * @param liveInstanceChangeListener listener to add.
   */
  void registerLiveInstanceChangeListener(LiveInstanceChangeListener liveInstanceChangeListener);
}
