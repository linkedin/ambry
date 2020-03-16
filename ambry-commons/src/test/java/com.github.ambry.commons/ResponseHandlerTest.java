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
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.server.ServerErrorCode;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * Test handler code
 */
public class ResponseHandlerTest {
  class DummyMap implements ClusterMap {

    ReplicaId lastReplicaID;
    Set<ReplicaEventType> lastReplicaEvents;

    public DummyMap() {
      lastReplicaEvents = new HashSet<>();
      lastReplicaID = null;
    }

    @Override
    public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
      return null;
    }

    @Override
    public List<PartitionId> getWritablePartitionIds(String partitionClass) {
      return null;
    }

    @Override
    public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
      return null;
    }

    @Override
    public List<PartitionId> getAllPartitionIds(String partitionClass) {
      return null;
    }

    @Override
    public boolean hasDatacenter(String datacenterName) {
      return false;
    }

    @Override
    public byte getLocalDatacenterId() {
      return UNKNOWN_DATACENTER_ID;
    }

    @Override
    public String getDatacenterName(byte id) {
      return null;
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

    @Override
    public JSONObject getSnapshot() {
      return null;
    }

    @Override
    public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
      return null;
    }

    @Override
    public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    }

    @Override
    public void close() {
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
    DummyMap mockClusterMap = new DummyMap();
    ResponseHandler handler = new ResponseHandler(mockClusterMap);

    Map<Object, ReplicaEventType[]> expectedEventTypes = new HashMap<>();

    expectedEventTypes.put(new SocketException(), new ReplicaEventType[]{ReplicaEventType.Node_Timeout});
    expectedEventTypes.put(new IOException(), new ReplicaEventType[]{ReplicaEventType.Node_Timeout});
    expectedEventTypes.put(new ConnectionPoolTimeoutException(""),
        new ReplicaEventType[]{ReplicaEventType.Node_Timeout});
    expectedEventTypes.put(ServerErrorCode.IO_Error,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Error});
    expectedEventTypes.put(ServerErrorCode.Disk_Unavailable,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Error});
    expectedEventTypes.put(ServerErrorCode.Partition_ReadOnly,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Ok,
            ReplicaEventType.Partition_ReadOnly, ReplicaEventType.Replica_Available});
    expectedEventTypes.put(ServerErrorCode.Replica_Unavailable,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Ok,
            ReplicaEventType.Replica_Unavailable});
    expectedEventTypes.put(ServerErrorCode.Temporarily_Disabled,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Ok,
            ReplicaEventType.Replica_Unavailable});
    expectedEventTypes.put(ServerErrorCode.Unknown_Error,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Ok,
            ReplicaEventType.Replica_Available});
    expectedEventTypes.put(ServerErrorCode.No_Error,
        new ReplicaEventType[]{ReplicaEventType.Node_Response, ReplicaEventType.Disk_Ok,
            ReplicaEventType.Replica_Available});
    expectedEventTypes.put(NetworkClientErrorCode.NetworkError, new ReplicaEventType[]{ReplicaEventType.Node_Timeout});
    expectedEventTypes.put(NetworkClientErrorCode.ConnectionUnavailable, new ReplicaEventType[]{});
    expectedEventTypes.put(new RouterException("", RouterErrorCode.UnexpectedInternalError), new ReplicaEventType[]{});
    expectedEventTypes.put(RouterErrorCode.AmbryUnavailable, new ReplicaEventType[]{});

    for (Map.Entry<Object, ReplicaEventType[]> entry : expectedEventTypes.entrySet()) {
      mockClusterMap.reset();
      handler.onEvent(new MockReplicaId(ReplicaType.DISK_BACKED), entry.getKey());
      Set<ReplicaEventType> expectedEvents = new HashSet<>(Arrays.asList(entry.getValue()));
      Set<ReplicaEventType> generatedEvents = mockClusterMap.getLastReplicaEvents();
      Assert.assertEquals(
          "Unexpected generated event for event " + entry.getKey() + " \nExpected: " + expectedEvents + " \nReceived: "
              + generatedEvents, expectedEvents, generatedEvents);
    }
  }
}
