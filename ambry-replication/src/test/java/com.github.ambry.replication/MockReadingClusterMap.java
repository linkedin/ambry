/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Mock clusterMap used when one wants the inputStream input for getPartitionIdFromStream
 * to be read and have constructed a MockPartitionId from the input
 */
public class MockReadingClusterMap implements ClusterMap {
  private boolean throwException = false;

  public MockReadingClusterMap() {
  }

  public void setThrowException(boolean bool) {
    this.throwException = bool;
  }

  public PartitionId getPartitionIdFromStream(InputStream inputStream) throws IOException {
    if (this.throwException) {
      throw new IOException();
    } else {
      byte[] bytes = new byte[10];
      inputStream.read(bytes);
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      bb.getShort();
      long num = bb.getLong();
      return new MockPartitionId(num, (String) null);
    }
  }

  public List<? extends PartitionId> getWritablePartitionIds(String partitionClass) {
    return null;
  }

  public List<? extends PartitionId> getAllPartitionIds(String partitionClass) {
    return null;
  }

  public boolean hasDatacenter(String s) {
    return false;
  }

  public byte getLocalDatacenterId() {
    return 0;
  }

  public String getDatacenterName(byte b) {
    return null;
  }

  public DataNodeId getDataNodeId(String s, int i) {
    return null;
  }

  public List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    return null;
  }

  public List<? extends DataNodeId> getDataNodeIds() {
    return null;
  }

  public MetricRegistry getMetricRegistry() {
    return null;
  }

  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType replicaEventType) {
  }

  public void close() {
  }
}
