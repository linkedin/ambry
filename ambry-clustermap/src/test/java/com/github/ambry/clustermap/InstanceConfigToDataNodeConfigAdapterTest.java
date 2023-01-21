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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils.ZkInfo;
import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.helix.InstanceType;
import org.junit.Test;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link InstanceConfigToDataNodeConfigAdapter}.
 */
public class InstanceConfigToDataNodeConfigAdapterTest extends DataNodeConfigSourceTestBase {
  private final ZkInfo zkInfo;
  private final ClusterMapConfig clusterMapConfig;
  private final MockHelixManager helixManager;

  public InstanceConfigToDataNodeConfigAdapterTest() throws Exception {
    String clusterName = "Cluster";
    File tempDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    tempDir.deleteOnExit();

    zkInfo = new ZkInfo(tempDirPath, DC_NAME, (byte) 0, 3333, true);
    String hostname = "localhost";
    int portNum = 1234;
    String selfInstanceName = getInstanceName(hostname, portNum);
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", hostname);
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", DC_NAME);
    props.setProperty("clustermap.port", Integer.toString(portNum));
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    helixManager =
        new MockHelixManager(selfInstanceName, InstanceType.SPECTATOR, "localhost:" + zkInfo.getPort(), clusterName,
            new MockHelixAdmin(), null, null, null, false);
    helixManager.connect();
  }

  /**
   * Test {@link DataNodeConfigSource} methods.
   */
  @Test
  public void testSetGetListener() throws Exception {
    InstanceConfigToDataNodeConfigAdapter source =
        new InstanceConfigToDataNodeConfigAdapter(helixManager, clusterMapConfig);
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

    // add a new host and add a second listener, both listeners should get all updates since MockHelixManager
    // always provides the complete set of instances
    reset(listener1);
    DataNodeConfig newConfig = createConfig(2, 2);
    allConfigs.add(newConfig);
    source.set(newConfig);
    DataNodeConfigChangeListener listener2 = mock(DataNodeConfigChangeListener.class);
    source.addDataNodeConfigChangeListener(listener2);
    checkListenerCall(listener1, allConfigs);
    checkListenerCall(listener2, allConfigs);

    // update an existing config
    reset(listener1);
    reset(listener2);
    DataNodeConfig updatedConfig = allConfigs.iterator().next();
    updatedConfig.getStoppedReplicas().add("partition");
    // add an extra map field to ensure it gets serialized/deserialized correctly
    updatedConfig.getExtraMapFields().put("extra", Collections.singletonMap("k", "v"));
    source.set(updatedConfig);
    // need to manually trigger a notification since MockHelixParticipant does not do so.
    // (other tests may not work as intended if we trigger a notification on calls to setInstanceConfig)
    helixManager.triggerConfigChangeNotification(false);
    checkListenerCall(listener1, allConfigs);
    checkListenerCall(listener2, allConfigs);

    // test get method for all configs
    for (DataNodeConfig config : allConfigs) {
      assertEquals("get() call returned incorrect result", config, source.get(config.getInstanceName()));
    }
    assertNull("Should not receive non-existent instance", source.get("abc"));
  }
}
