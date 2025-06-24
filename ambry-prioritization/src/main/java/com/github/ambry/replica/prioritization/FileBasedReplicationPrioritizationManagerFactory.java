/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.config.ReplicaPrioritizationStrategy;
import com.github.ambry.replica.prioritization.disruption.DisruptionService;


public class FileBasedReplicationPrioritizationManagerFactory implements PrioritizationManagerFactory {
  final DisruptionService disruptionService;
  final ClusterManagerQueryHelper clusterManagerQueryHelper;
  final String datacenterName;

  public FileBasedReplicationPrioritizationManagerFactory() {
    this(null, null, null);
  }

  public FileBasedReplicationPrioritizationManagerFactory(DisruptionService disruptionService,
      ClusterManagerQueryHelper clusterManagerQueryHelper, String datacenterName) {
    this.disruptionService = disruptionService;
    this.clusterManagerQueryHelper = clusterManagerQueryHelper;
    this.datacenterName = datacenterName;
  }

  @Override
  public PrioritizationManager getPrioritizationManager(ReplicaPrioritizationStrategy replicaPrioritizationStrategy) {
    if (replicaPrioritizationStrategy == ReplicaPrioritizationStrategy.FirstComeFirstServe) {
      return new FCFSPrioritizationManager();
    } else if (replicaPrioritizationStrategy == ReplicaPrioritizationStrategy.ACMAdvanceNotificationsBased) {
      return new FileCopyDisruptionBasedPrioritizationManager(disruptionService, datacenterName,
          clusterManagerQueryHelper);
    } else {
      return null;
    }
  }
}