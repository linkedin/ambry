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
package com.github.ambry.commons;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test handler code
 */
public class ResponseHandlerTest {
  class DummyMap implements ClusterMap {

    ReplicaId lastReplicaID;
    Set<ReplicaEventType> lastReplicaEvents;

    public DummyMap() {
      lastReplicaEvents = new HashSet<ReplicaEventType>();
      lastReplicaID = null;
    }

    @Override
    public PartitionId getPartitionIdFromStream(DataInputStream stream)
        throws IOException {
      return null;
    }

    @Override
    public List<PartitionId> getWritablePartitionIds() {
      return null;
    }

    @Override
    public boolean hasDatacenter(String datacenterName) {
      return false;
    }

    @Override
    public DataNodeId getDataNodeId(String hostname, int port) {
      return null;
    }

    @Override
    public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
      return null;
    }

    @Override
    public List<DataNodeId> getDataNodeIds() {
      return null;
    }

    @Override
    public MetricRegistry getMetricRegistry() {
      return null;
    }

    @Override
    public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
      lastReplicaID = replicaId;
      lastReplicaEvents.add(event);
    }

    public void reset() {
      lastReplicaID = null;
      lastReplicaEvents.clear();
    }

    public ReplicaId getLastReplicaID() {
      return lastReplicaID;
    }

    public Set<ReplicaEventType> getLastReplicaEvents() {
      return lastReplicaEvents;
    }
  }

  @Test
  public void basicTest() {
    DummyMap map = new DummyMap();
    ResponseHandler handler = new ResponseHandler(map);
    handler.onRequestResponseException(new MockReplicaId(), new SocketException());
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Timeout));
    map.reset();
    handler.onRequestResponseException(new MockReplicaId(), new IOException());
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Timeout));
    map.reset();
    handler.onRequestResponseException(new MockReplicaId(), new ConnectionPoolTimeoutException("test"));
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Timeout));
    map.reset();
    handler.onRequestResponseError(new MockReplicaId(), ServerErrorCode.IO_Error);
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Response));
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Disk_Error));
    map.reset();
    handler.onRequestResponseError(new MockReplicaId(), ServerErrorCode.Disk_Unavailable);
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Response));
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Disk_Error));
    map.reset();
    handler.onRequestResponseError(new MockReplicaId(), ServerErrorCode.Partition_ReadOnly);
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Response));
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Disk_Ok));
    map.reset();
    handler.onRequestResponseError(new MockReplicaId(), ServerErrorCode.Unknown_Error);
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Node_Response));
    Assert.assertTrue(map.getLastReplicaEvents().contains(ReplicaEventType.Disk_Ok));
  }
}
