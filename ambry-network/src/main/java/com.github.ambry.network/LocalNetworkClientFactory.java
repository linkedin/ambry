/**
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
package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A factory class used to get new instances of a {@link LocalNetworkClient}
 */
public class LocalNetworkClientFactory implements NetworkClientFactory {
  protected final NetworkMetrics networkMetrics;
  protected final NetworkConfig networkConfig;
  private final LocalRequestResponseChannel channel;
  private final Time time;
  private final AtomicInteger processorIdGenerator;

  /**
   * Construct a factory using the given parameters.
   * @param networkMetrics the metrics for the Network layer.
   * @param channel the channel to use.
   * @param time the Time instance to use.
   */
  public LocalNetworkClientFactory(LocalRequestResponseChannel channel, NetworkConfig networkConfig,
      NetworkMetrics networkMetrics, Time time) {
    this.channel = channel;
    this.networkMetrics = networkMetrics;
    this.networkConfig = networkConfig;
    this.time = time;
    processorIdGenerator = new AtomicInteger(0);
  }

  /**
   * Construct and return a new {@link LocalNetworkClient}
   * @return return a new {@link LocalNetworkClient}
   */
  @Override
  public LocalNetworkClient getNetworkClient() {
    // Every client created by this factory gets a unique processorId
    return new LocalNetworkClient(channel, processorIdGenerator.getAndIncrement(), networkMetrics, time);
  }
}
