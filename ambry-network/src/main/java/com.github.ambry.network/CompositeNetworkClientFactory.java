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

package com.github.ambry.network;

import com.github.ambry.clustermap.ReplicaType;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;


/**
 * An implementation of {@link NetworkClient} that returns a {@link CompositeNetworkClient}, which routes requests to
 * child clients based on the {@link ReplicaType} in the provided requests.
 */
public class CompositeNetworkClientFactory implements NetworkClientFactory {
  EnumMap<ReplicaType, NetworkClientFactory> childNetworkClientFactories;

  /**
   * @param childNetworkClientFactories a map from {@link ReplicaType} to the {@link NetworkClientFactory} to use for
   *                                    that type.
   */
  public CompositeNetworkClientFactory(Map<ReplicaType, NetworkClientFactory> childNetworkClientFactories) {
    this.childNetworkClientFactories = new EnumMap<>(childNetworkClientFactories);
  }

  @Override
  public NetworkClient getNetworkClient() throws IOException {
    EnumMap<ReplicaType, NetworkClient> childNetworkClients = new EnumMap<>(ReplicaType.class);
    for (Map.Entry<ReplicaType, NetworkClientFactory> entry : childNetworkClientFactories.entrySet()) {
      childNetworkClients.put(entry.getKey(), entry.getValue().getNetworkClient());
    }
    return new CompositeNetworkClient(childNetworkClients);
  }
}
