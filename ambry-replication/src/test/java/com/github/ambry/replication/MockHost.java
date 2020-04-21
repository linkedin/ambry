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
package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.utils.SystemTime;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Representation of a host. Contains all the data for all partitions.
 */
public class MockHost {
  final Map<PartitionId, InMemoryStore> storesByPartition = new HashMap<>();
  private final ClusterMap clusterMap;

  public final DataNodeId dataNodeId;
  // creat a list to track number of replicas in each metadata request
  final List<Integer> replicaCountPerRequestTracker = new ArrayList<>();
  final Map<PartitionId, List<MessageInfo>> infosByPartition = new HashMap<>();
  final Map<PartitionId, List<ByteBuffer>> buffersByPartition = new HashMap<>();

  MockHost(DataNodeId dataNodeId, ClusterMap clusterMap) {
    this.dataNodeId = dataNodeId;
    this.clusterMap = clusterMap;
  }

  /**
   * Adds a message to {@code id} with the provided details.
   * @param id the {@link PartitionId} to add the message to.
   * @param messageInfo the {@link MessageInfo} of the message.
   * @param buffer the data accompanying the message.
   */
  public void addMessage(PartitionId id, MessageInfo messageInfo, ByteBuffer buffer) {
    infosByPartition.computeIfAbsent(id, id1 -> new ArrayList<>()).add(messageInfo);
    buffersByPartition.computeIfAbsent(id, id1 -> new ArrayList<>()).add(buffer.duplicate());
  }

  /**
   * Gets the list of {@link RemoteReplicaInfo} from this host to the given {@code remoteHost}
   * @param remoteHost the host whose replica info is required.
   * @param listener the {@link ReplicationTest.StoreEventListener} to use.
   * @return the list of {@link RemoteReplicaInfo} from this host to the given {@code remoteHost}
   */
  List<RemoteReplicaInfo> getRemoteReplicaInfos(MockHost remoteHost, ReplicationTest.StoreEventListener listener) {
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    for (ReplicaId replicaId : replicaIds) {
      for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
        if (peerReplicaId.getDataNodeId().equals(remoteHost.dataNodeId)) {
          PartitionId partitionId = replicaId.getPartitionId();
          InMemoryStore store = storesByPartition.computeIfAbsent(partitionId,
              partitionId1 -> new InMemoryStore(partitionId, infosByPartition.computeIfAbsent(partitionId1,
                  (Function<PartitionId, List<MessageInfo>>) partitionId2 -> new ArrayList<>()),
                  buffersByPartition.computeIfAbsent(partitionId1,
                      (Function<PartitionId, List<ByteBuffer>>) partitionId22 -> new ArrayList<>()), listener));
          RemoteReplicaInfo remoteReplicaInfo =
              new RemoteReplicaInfo(peerReplicaId, replicaId, store, new MockFindToken(0, 0), Long.MAX_VALUE,
                  SystemTime.getInstance(), new Port(peerReplicaId.getDataNodeId().getPort(), PortType.PLAINTEXT));
          remoteReplicaInfos.add(remoteReplicaInfo);
        }
      }
    }
    return remoteReplicaInfos;
  }

  /**
   * Gets the message infos that are present in this host but missing in {@code other}.
   * @param other the list of {@link MessageInfo} to check against.
   * @return the message infos that are present in this host but missing in {@code other}.
   */
  Map<PartitionId, List<MessageInfo>> getMissingInfos(Map<PartitionId, List<MessageInfo>> other) throws Exception {
    return getMissingInfos(other, null);
  }

  /**
   * Gets the message infos that are present in this host but missing in {@code other}.
   * @param other the list of {@link MessageInfo} to check against.
   * @return the message infos that are present in this host but missing in {@code other}.
   */
  Map<PartitionId, List<MessageInfo>> getMissingInfos(Map<PartitionId, List<MessageInfo>> other,
      StoreKeyConverter storeKeyConverter) throws Exception {
    Map<PartitionId, List<MessageInfo>> missingInfos = new HashMap<>();
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : infosByPartition.entrySet()) {
      PartitionId partitionId = entry.getKey();
      for (MessageInfo messageInfo : entry.getValue()) {
        boolean found = false;
        StoreKey convertedKey;
        if (storeKeyConverter == null) {
          convertedKey = messageInfo.getStoreKey();
        } else {
          Map<StoreKey, StoreKey> map = storeKeyConverter.convert(Collections.singletonList(messageInfo.getStoreKey()));
          convertedKey = map.get(messageInfo.getStoreKey());
          if (convertedKey == null) {
            continue;
          }
        }
        for (MessageInfo otherInfo : other.get(partitionId)) {
          if (convertedKey.equals(otherInfo.getStoreKey()) && messageInfo.isDeleted() == otherInfo.isDeleted()) {
            found = true;
            break;
          }
        }
        if (!found) {
          missingInfos.computeIfAbsent(partitionId, partitionId1 -> new ArrayList<>()).add(messageInfo);
        }
      }
    }
    return missingInfos;
  }

  /**
   * Gets the buffers that are present in this host but missing in {@code other}.
   * @param other the list of {@link ByteBuffer} to check against.
   * @return the buffers that are present in this host but missing in {@code other}.
   */
  Map<PartitionId, List<ByteBuffer>> getMissingBuffers(Map<PartitionId, List<ByteBuffer>> other) {
    Map<PartitionId, List<ByteBuffer>> missingBuffers = new HashMap<>();
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : buffersByPartition.entrySet()) {
      PartitionId partitionId = entry.getKey();
      for (ByteBuffer buf : entry.getValue()) {
        boolean found = false;
        for (ByteBuffer bufActual : other.get(partitionId)) {
          if (Arrays.equals(buf.array(), bufActual.array())) {
            found = true;
            break;
          }
        }
        if (!found) {
          missingBuffers.computeIfAbsent(partitionId, partitionId1 -> new ArrayList<>()).add(buf);
        }
      }
    }
    return missingBuffers;
  }
}
