/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.utils.Pair;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an iterator for account storage stats fetched from MySQL
 */
public class AccountStorageStatsIterator implements Iterator<Pair<String, HostAccountStorageStatsWrapper>> {

  private static final Logger logger = LoggerFactory.getLogger(AccountStorageStatsIterator.class);
  private final Iterator<String> instances;
  private final AccountStatsStore accountStatsStore;
  private final ClusterMapConfig clusterMapConfig;

  public AccountStorageStatsIterator(List<String> instances, AccountStatsStore accountStatsStore, ClusterMapConfig clusterMapConfig) {
    this.instances = instances.iterator();
    this.accountStatsStore = accountStatsStore;
    this.clusterMapConfig = clusterMapConfig;
  }

  @Override
  public boolean hasNext() {
    return instances.hasNext();
  }

  @Override
  public Pair<String, HostAccountStorageStatsWrapper> next() {
    String hostname = instances.next();
    try {
      Pair<String, Integer> hostNameAndPort = TaskUtils.getHostNameAndPort(hostname, clusterMapConfig.clusterMapPort);
      return new Pair<>(hostname,
          accountStatsStore.queryHostAccountStorageStatsByHost(hostNameAndPort.getFirst(), hostNameAndPort.getSecond()));
    } catch (Exception e) {
      logger.error("Failed to get account storage stats for {}", hostname);
      throw new RuntimeException(e);
    }
  }
}
