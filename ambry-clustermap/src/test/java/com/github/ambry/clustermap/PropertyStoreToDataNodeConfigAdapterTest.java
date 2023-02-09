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

import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.junit.After;
import org.junit.Test;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test {@link PropertyStoreToDataNodeConfigAdapter}.
 */
public class PropertyStoreToDataNodeConfigAdapterTest extends DataNodeConfigSourceTestBase {
  private final ZkInfo zkInfo;
  private final String zkAddress;
  private final ClusterMapConfig clusterMapConfig;
  private final PropertyStoreToDataNodeConfigAdapter source;

  /**
   * Start a local zookeeper instance.
   */
  public PropertyStoreToDataNodeConfigAdapterTest() throws IOException {
    zkInfo = new ZkInfo(getTempDir("propertyStoreTest"), DC_NAME, (byte) 1, 4321, true);
    zkAddress = "localhost:" + zkInfo.getPort();
    clusterMapConfig = TestUtils.getDummyConfig();
    source = new PropertyStoreToDataNodeConfigAdapter(zkAddress, clusterMapConfig);
  }

  /**
   * Close the {@link PropertyStoreToDataNodeConfigAdapter} and shut down the local ZK server.
   */
  @After
  public void cleanup() {
    try {
      source.close();
    } finally {
      zkInfo.shutdown();
    }
  }

  /**
   * Test {@link DataNodeConfigSource} methods.
   */
  @Test
  public void testSetGetListener() throws Exception {
    // set up 10 node configs and attach a listener.
    Set<DataNodeConfig> allConfigs = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      DataNodeConfig config = createConfig(i, 9 - i);
      allConfigs.add(config);
      source.set(config);
    }
    DataNodeConfigChangeListener listener1 = mock(DataNodeConfigChangeListener.class);
    source.addDataNodeConfigChangeListener(listener1);
    checkListenerCall(listener1, allConfigs);

    // add a new host and add a second listener, second listener should get all updates, first should get new host only.
    reset(listener1);
    DataNodeConfig newConfig = createConfig(2, 2);
    allConfigs.add(newConfig);
    source.set(newConfig);
    DataNodeConfigChangeListener listener2 = mock(DataNodeConfigChangeListener.class);
    source.addDataNodeConfigChangeListener(listener2);
    checkListenerCall(listener1, Collections.singleton(newConfig));
    checkListenerCall(listener2, allConfigs);

    // update an existing config
    reset(listener1);
    reset(listener2);
    DataNodeConfig updatedConfig = allConfigs.iterator().next();
    updatedConfig.getStoppedReplicas().add("partition");
    source.set(updatedConfig);
    checkListenerCall(listener1, Collections.singleton(updatedConfig));
    checkListenerCall(listener2, Collections.singleton(updatedConfig));

    // delete an existing config using another ZK client, this should be a no-op for listeners now.
    reset(listener1);
    reset(listener2);
    String rootPath = PropertyPathBuilder.propertyStore(clusterMapConfig.clusterMapClusterName);
    HelixPropertyStore<ZNRecord> propertyStore =
        CommonUtils.createHelixPropertyStore(zkAddress, rootPath, Collections.emptyList());
    try {
      assertTrue("Remove did not succeed",
          propertyStore.remove(PropertyStoreToDataNodeConfigAdapter.CONFIG_PATH + "/" + updatedConfig.getInstanceName(),
              AccessOption.PERSISTENT));
    } finally {
      propertyStore.stop();
    }
    allConfigs.remove(updatedConfig);
    // removal should be reflected in get() call (with some delay since this property store is informed through a watch)
    assertTrue("Node removal did not propagate",
        checkAndSleep(null, () -> source.get(updatedConfig.getInstanceName()), 2000));
    verify(listener1, never()).onDataNodeConfigChange(any());
    verify(listener2, never()).onDataNodeConfigChange(any());

    // test get method for all configs
    for (DataNodeConfig config : allConfigs) {
      assertEquals("get() call returned incorrect result", config, source.get(config.getInstanceName()));
    }
  }

  /**
   * Test {@link DataNodeConfigSource} remove method.
   */
  @Test
  public void testRemoveListener() throws Exception {
    // Add a data node config to property store
    DataNodeConfig config = createConfig(1, 1);
    source.set(config);

    // Add a listener and verify current data node config is informed
    DataNodeConfigChangeListener listener = mock(DataNodeConfigChangeListener.class);
    source.addDataNodeConfigChangeListener(listener);
    checkListenerCall(listener, Collections.singleton(config));

    // Remove data node config from property store and verify listener is invoked.
    reset(listener);
    source.remove(config.getInstanceName());
    verify(listener, timeout(500)).onDataNodeDelete(
        argThat(instanceName -> instanceName.equals(config.getInstanceName())));
  }

  /**
   * Test handling of listener errors during initialization and updates.
   */
  @Test
  public void testListenerExceptions() throws Exception {
    // exception during init
    DataNodeConfigChangeListener listener1 = mock(DataNodeConfigChangeListener.class);
    RuntimeException initException = new RuntimeException("Failure during init");
    doThrow(initException).when(listener1).onDataNodeConfigChange(any());
    assertException(RuntimeException.class, () -> source.addDataNodeConfigChangeListener(listener1),
        e -> assertEquals("Unexpected exception thrown", initException, e));
    checkListenerCall(listener1, Collections.emptySet());
    // no more calls to listener should occur
    reset(listener1);
    DataNodeConfig config = createConfig(1, 1);
    source.set(config);
    verify(listener1, never()).onDataNodeConfigChange(any());

    // exception during update call.
    DataNodeConfigChangeListener listener2 = mock(DataNodeConfigChangeListener.class);
    source.addDataNodeConfigChangeListener(listener2);
    checkListenerCall(listener2, Collections.singleton(config));
    reset(listener2);
    RuntimeException updateException = new RuntimeException("Failure during update");
    doThrow(updateException).when(listener2).onDataNodeConfigChange(any());
    config.getDisabledReplicas().add("partition1");
    source.set(config);
    checkListenerCall(listener2, Collections.singleton(config));
    // more updates should still create notifications
    config.getDisabledReplicas().add("partition2");
    source.set(config);
    checkListenerCall(listener2, Collections.singleton(config));
  }
}
