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

package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An implementation of {@link DataNodeConfigSource} that reads {@link ZNRecord}s stored under the
 * "/DataNodeConfigs" path in a {@link HelixPropertyStore}.
 */
public class PropertyStoreToDataNodeConfigAdapter implements DataNodeConfigSource {
  static final String CONFIG_PATH = "/DataNodeConfigs";
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyStoreToDataNodeConfigAdapter.class);
  private final HelixPropertyStore<ZNRecord> propertyStore;
  private final Converter converter;
  private final Executor eventExecutor;

  /**
   * @param propertyStore the {@link HelixPropertyStore} instance to use to interact with zookeeper.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   */
  public PropertyStoreToDataNodeConfigAdapter(HelixPropertyStore<ZNRecord> propertyStore,
      ClusterMapConfig clusterMapConfig) {
    this.propertyStore = propertyStore;
    this.converter = new Converter(clusterMapConfig.clusterMapDefaultPartitionClass);
    this.eventExecutor = Executors.newSingleThreadExecutor();
  }

  @Override
  public void addDataNodeConfigChangeListener(DataNodeConfigChangeListener listener) throws Exception {
    Subscription subscription = new Subscription(listener);
    propertyStore.subscribe(CONFIG_PATH, subscription);
    subscription.start();
  }

  @Override
  public boolean set(DataNodeConfig config) {
    ZNRecord record = converter.convert(config);
    String path = CONFIG_PATH + "/" + record.getId();
    return propertyStore.set(path, record, AccessOption.PERSISTENT);
  }

  @Override
  public DataNodeConfig get(String instanceName) {
    String path = CONFIG_PATH + "/" + instanceName;
    ZNRecord record = propertyStore.get(path, new Stat(), AccessOption.PERSISTENT);
    return record != null ? converter.convert(record) : null;
  }

  private class Subscription implements HelixPropertyListener {
    private final DataNodeConfigChangeListener listener;
    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();

    Subscription(DataNodeConfigChangeListener listener) {
      this.listener = listener;
    }

    /**
     * Tell the background executor to perform any initialization needed. This should be called after registering this
     * listener using {@link HelixPropertyStore#subscribe}. This will ensure that the first event sent to
     * {@link #listener} will contain the entire set of current configs.
     * @throws Exception if there was an exception while making the initialization call.
     */
    void start() throws Exception {
      eventExecutor.execute(this::initializeIfNeeded);
      try {
        initFuture.get();
      } catch (ExecutionException e) {
        throw Utils.extractExecutionExceptionCause(e);
      }
    }

    @Override
    public void onDataChange(String path) {
      LOGGER.debug("DataNodeConfig path {} changed", path);
      onPathChange(path);
    }

    @Override
    public void onDataCreate(String path) {
      LOGGER.debug("DataNodeConfig path {} created", path);
      onPathChange(path);
    }

    @Override
    public void onDataDelete(String path) {
      // TODO handle node deletions dynamically. Doing so requires further work in the ClusterChangeHandler impl
      LOGGER.info("DataNodeConfig path {} deleted. This requires a restart to handle", path);
    }

    private void onPathChange(String path) {
      eventExecutor.execute(() -> updateOrInitialize(path));
    }

    private Iterable<DataNodeConfig> lazyIterable(Collection<ZNRecord> records) {
      return () -> records.stream().map(converter::convert).filter(Objects::nonNull).iterator();
    }

    /**
     * Either perform a full initialization or notify the listener about an incremental update to a path. This should be
     * called from the event executor thread.
     * @param changedPath the path that changed.
     */
    private synchronized void updateOrInitialize(String changedPath) {
      try {
        if (initializeIfNeeded()) {
          ZNRecord record = propertyStore.get(changedPath, null, AccessOption.PERSISTENT);
          if (record != null) {
            listener.onDataNodeConfigChange(lazyIterable(Collections.singleton(record)));
          } else {
            LOGGER.info("DataNodeConfig at path {} not found", changedPath);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Exception during DataNodeConfig change", e);
      }
    }

    /**
     * If initialization has not yet been completed successfully, attempt an initialization. This should be called from
     * the event executor thread.
     * @return {@code true} if a partial update can be performed following this call. {@code false} if it is either
     *         not needed since a full init was just done or if an initialization error occurred previously.
     */
    private synchronized boolean initializeIfNeeded() {
      boolean partialUpdateAllowed = false;
      try {
        if (initFuture.isDone()) {
          partialUpdateAllowed = !initFuture.isCompletedExceptionally();
        } else {
          List<ZNRecord> records = propertyStore.getChildren(CONFIG_PATH, null, AccessOption.PERSISTENT);
          listener.onDataNodeConfigChange(lazyIterable(records));
          initFuture.complete(null);
        }
      } catch (Throwable t) {
        initFuture.completeExceptionally(t);
      }
      return partialUpdateAllowed;
    }
  }

  /**
   * Convert between {@link DataNodeConfig}s and {@link ZNRecord}s.
   */
  static class Converter {
    private static final int VERSION_0 = 0;
    private static final String MOUNT_PREFIX = "mount-";
    private static final String HOSTNAME_FIELD = "hostname";
    private static final String PORT_FIELD = "port";
    private final String defaultPartitionClass;

    /**
     * @param defaultPartitionClass the default partition class for these nodes.
     */
    Converter(String defaultPartitionClass) {
      this.defaultPartitionClass = defaultPartitionClass;
    }

    /**
     * @param record the {@link ZNRecord} to convert to a {@link DataNodeConfig} object.
     * @return the {@link DataNodeConfig}, or {@code null} if the {@link ZNRecord} provided has an unsupported schema
     *         version.
     */
    DataNodeConfig convert(ZNRecord record) {
      int schemaVersion = getSchemaVersion(record);
      if (schemaVersion != VERSION_0) {
        LOGGER.warn("Unknown InstanceConfig schema version {} in {}. Ignoring.", schemaVersion, record);
        return null;
      }

      DataNodeConfig dataNodeConfig = new DataNodeConfig(record.getId(), record.getSimpleField(HOSTNAME_FIELD),
          record.getIntField(PORT_FIELD, DataNodeId.UNKNOWN_PORT), getDcName(record), getSslPortStr(record),
          getHttp2PortStr(record), getRackId(record), DEFAULT_XID);
      dataNodeConfig.getSealedReplicas().addAll(getSealedReplicas(record));
      dataNodeConfig.getStoppedReplicas().addAll(getStoppedReplicas(record));
      dataNodeConfig.getDisabledReplicas().addAll(getDisabledReplicas(record));
      record.getMapFields().forEach((key, diskProps) -> {
        if (key.startsWith(MOUNT_PREFIX)) {
          String mountPath = key.substring(MOUNT_PREFIX.length());
          DataNodeConfig.DiskConfig disk = new DataNodeConfig.DiskConfig(
              diskProps.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE,
              Long.parseLong(diskProps.get(DISK_CAPACITY_STR)));
          String replicasStr = diskProps.get(REPLICAS_STR);
          if (!Utils.isNullOrEmpty(replicasStr)) {
            for (String replicaStr : replicasStr.split(REPLICAS_DELIM_STR)) {
              String[] replicaStrParts = replicaStr.split(REPLICAS_STR_SEPARATOR);
              // partition name and replica name are the same.
              String partitionName = replicaStrParts[0];
              long replicaCapacity = Long.parseLong(replicaStrParts[1]);
              String partitionClass = replicaStrParts.length > 2 ? replicaStrParts[2] : defaultPartitionClass;
              disk.getReplicaConfigs()
                  .put(partitionName, new DataNodeConfig.ReplicaConfig(replicaCapacity, partitionClass));
            }
          }
          dataNodeConfig.getDiskConfigs().put(mountPath, disk);
        }
      });
      return dataNodeConfig;
    }

    /**
     * @param dataNodeConfig the {@link DataNodeConfig} to convert to a {@link ZNRecord} that can be stored in the
     *                       property store.
     * @return the {@link ZNRecord}.
     */
    ZNRecord convert(DataNodeConfig dataNodeConfig) {
      ZNRecord record = new ZNRecord(dataNodeConfig.getInstanceName());
      record.setIntField(SCHEMA_VERSION_STR, VERSION_0);
      record.setSimpleField(HOSTNAME_FIELD, dataNodeConfig.getHostName());
      record.setIntField(PORT_FIELD, dataNodeConfig.getPort());
      record.setSimpleField(DATACENTER_STR, dataNodeConfig.getDatacenterName());
      if (dataNodeConfig.getSslPort() != null) {
        record.setIntField(SSL_PORT_STR, dataNodeConfig.getSslPort());
      }
      if (dataNodeConfig.getHttp2Port() != null) {
        record.setIntField(HTTP2_PORT_STR, dataNodeConfig.getHttp2Port());
      }
      record.setSimpleField(RACKID_STR, dataNodeConfig.getRackId());
      record.setListField(SEALED_STR, new ArrayList<>(dataNodeConfig.getSealedReplicas()));
      record.setListField(STOPPED_REPLICAS_STR, new ArrayList<>(dataNodeConfig.getStoppedReplicas()));
      record.setListField(DISABLED_REPLICAS_STR, new ArrayList<>(dataNodeConfig.getDisabledReplicas()));
      dataNodeConfig.getDiskConfigs().forEach((mountPath, diskConfig) -> {
        Map<String, String> diskProps = new HashMap<>();
        diskProps.put(DISK_STATE, diskConfig.getState() == HardwareState.AVAILABLE ? AVAILABLE_STR : UNAVAILABLE_STR);
        diskProps.put(DISK_CAPACITY_STR, Long.toString(diskConfig.getDiskCapacityInBytes()));
        StringBuilder replicasStrBuilder = new StringBuilder();
        diskConfig.getReplicaConfigs()
            .forEach((partitionName, replicaConfig) -> replicasStrBuilder.append(partitionName)
                .append(REPLICAS_STR_SEPARATOR)
                .append(replicaConfig.getReplicaCapacityInBytes())
                .append(REPLICAS_STR_SEPARATOR)
                .append(replicaConfig.getPartitionClass())
                .append(REPLICAS_DELIM_STR));
        diskProps.put(REPLICAS_STR, replicasStrBuilder.toString());
        record.setMapField(MOUNT_PREFIX + mountPath, diskProps);
      });
      return record;
    }
  }
}
