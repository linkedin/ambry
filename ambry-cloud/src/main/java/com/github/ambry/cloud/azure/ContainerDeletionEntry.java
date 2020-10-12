/**
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
 */
package com.github.ambry.cloud.azure;

import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONObject;


/**
 * Class representing container deletion status in cloud.
 */
public class ContainerDeletionEntry {

  static final String VERSION_KEY = "version";
  static final String CONTAINER_ID_KEY = "containerId";
  static final String ACCOUNT_ID_KEY = "accountId";
  static final String CONTAINER_DELETE_TRIGGER_TIME_KEY = "deleteTriggerTime";
  static final String IS_DELETED_KEY = "isDeleted";
  static final String DELETE_PENDING_PARTITIONS_KEY = "deletePendingPartitions";

  private static short JSON_VERSION_1 = 1;

  private final short version;
  private final short containerId;
  private final short accountId;
  private final Set<String> deletePendingPartitions;
  private final long deleteTriggerTimestamp;
  private boolean isDeleted;

  /**
   * Constructor for {@link ContainerDeletionEntry}.
   * @param containerId container id.
   * @param accountId account id of the container.
   * @param deleteTriggerTimestamp timestamp at which delete was triggered.
   * @param isDeleted {@code true} if all container blobs are deleted in cloud. {@code false} otherwise.
   * @param partitionIds {@link Collection} of all the cloud partition ids from which container is yet to be deleted.
   */
  public ContainerDeletionEntry(short containerId, short accountId, long deleteTriggerTimestamp, boolean isDeleted,
      Collection<String> partitionIds) {
    this.version = JSON_VERSION_1;
    this.containerId = containerId;
    this.accountId = accountId;
    this.deleteTriggerTimestamp = deleteTriggerTimestamp;
    this.isDeleted = isDeleted;
    if (!isDeleted) {
      deletePendingPartitions = new HashSet<>();
      deletePendingPartitions.addAll(partitionIds);
    } else {
      deletePendingPartitions = Collections.emptySet();
    }
  }

  /**
   * Private constructor for {@link ContainerDeletionEntry}. Used for deserialization.
   * @param version deserialized version.
   * @param containerId container id.
   * @param accountId account id of the container.
   * @param deleteTriggerTimestamp timestamp at which delete was triggered.
   * @param isDeleted {@code true} if all container blobs are deleted in cloud. {@code false} otherwise.
   * @param pendingPartitions {@link Collection} of all the cloud partition ids from which container is yet to be deleted.
   */
  private ContainerDeletionEntry(short version, short containerId, short accountId, long deleteTriggerTimestamp,
      boolean isDeleted, Collection<Object> pendingPartitions) {
    this.version = version;
    this.containerId = containerId;
    this.accountId = accountId;
    this.deleteTriggerTimestamp = deleteTriggerTimestamp;
    this.isDeleted = isDeleted;
    this.deletePendingPartitions = new HashSet<>();
    pendingPartitions.forEach(partitionId -> this.deletePendingPartitions.add((String) partitionId));
  }

  /**
   * Create a {@link ContainerDeletionEntry} from specified {@link Container} in specified {@link ClusterMap}.
   * @param container {@link Container} from which to create deletion entry.
   * @param partitionIds {@link Collection} of partition ids.
   * @return {@link ContainerDeletionEntry} object.
   */
  public static ContainerDeletionEntry fromContainer(Container container, Collection<String> partitionIds) {
    return new ContainerDeletionEntry(container.getId(), container.getParentAccountId(),
        container.getDeleteTriggerTime(), false, partitionIds);
  }

  /**
   * Create {@link ContainerDeletionEntry} from specified json.
   * @param jsonObject {@link JSONObject} representing the serialized {@link ContainerDeletionEntry}.
   * @return deserialized {@link ContainerDeletionEntry} object.
   */
  public static ContainerDeletionEntry fromJson(JSONObject jsonObject) {
    return new ContainerDeletionEntry((short) jsonObject.getInt(VERSION_KEY),
        (short) jsonObject.getInt(CONTAINER_ID_KEY), (short) jsonObject.getInt(ACCOUNT_ID_KEY),
        jsonObject.getLong(CONTAINER_DELETE_TRIGGER_TIME_KEY), jsonObject.getBoolean(IS_DELETED_KEY),
        jsonObject.getJSONArray(DELETE_PENDING_PARTITIONS_KEY).toList());
  }

  /**
   * Mark the container as deleted in cloud.
   */
  public void markDeleted() {
    isDeleted = true;
  }

  /**
   * Remove a delete pending partition.
   * @param partitionId partition to remove.
   */
  public void removePartition(String partitionId) {
    deletePendingPartitions.remove(partitionId);
  }

  /**
   * @return deletion status of the container.
   */
  public boolean isDeleted() {
    return isDeleted;
  }

  /**
   * @return {@code containerId} of the container.
   */
  public short getContainerId() {
    return containerId;
  }

  /**
   * @return {@code accountId} of the container.
   */
  public short getAccountId() {
    return accountId;
  }

  /**
   * @return delete trigger timestamp when the container deletion was triggered by customer.
   */
  public long getDeleteTriggerTimestamp() {
    return deleteTriggerTimestamp;
  }

  /**
   * Serialize {@link Container} object to save to Cosmos.
   * @return serialized {@link JSONObject}.
   */
  public JSONObject toJson() {
    JSONObject metadata = new JSONObject();
    metadata.put(VERSION_KEY, version);
    metadata.put(CONTAINER_ID_KEY, containerId);
    metadata.put(ACCOUNT_ID_KEY, accountId);
    metadata.put(IS_DELETED_KEY, isDeleted);
    metadata.put(CONTAINER_DELETE_TRIGGER_TIME_KEY, deleteTriggerTimestamp);
    metadata.put(DELETE_PENDING_PARTITIONS_KEY, deletePendingPartitions);
    return metadata;
  }
}
