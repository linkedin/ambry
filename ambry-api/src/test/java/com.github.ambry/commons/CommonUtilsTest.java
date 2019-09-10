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

package com.github.ambry.commons;

import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.junit.Test;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


public class CommonUtilsTest {

  /**
   * Tests for {@link CommonUtils#createHelixPropertyStore(String, HelixPropertyStoreConfig, List)}.
   */
  @Test
  public void createHelixPropertyStoreTest() throws IOException {
    Properties storeProps = new Properties();
    storeProps.setProperty("helix.property.store.root.path", "/Ambry-Test/" + ClusterMapUtils.PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    String tempDirPath = getTempDir("clusterMapUtils-");
    ZkInfo zkInfo = new ZkInfo(tempDirPath, "DC1", (byte) 0, 2200, true);

    try {
      CommonUtils.createHelixPropertyStore(null, propertyStoreConfig, Collections.emptyList());
      fail("create HelixPropertyStore with invalid arguments should fail");
    } catch (IllegalArgumentException e) {
      //expected
    }

    try {
      CommonUtils.createHelixPropertyStore("", propertyStoreConfig, Collections.emptyList());
      fail("create HelixPropertyStore with invalid arguments should fail");
    } catch (IllegalArgumentException e) {
      //expected
    }

    try {
      CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), null, Collections.emptyList());
      fail("create HelixPropertyStore with invalid arguments should fail");
    } catch (IllegalArgumentException e) {
      //expected
    }

    HelixPropertyStore<ZNRecord> propertyStore =
        CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
            Collections.singletonList(propertyStoreConfig.rootPath));
    assertNotNull(propertyStore);

    // Ensure the HelixPropertyStore works correctly
    List<String> list = Arrays.asList("first", "second", "third");
    String path = propertyStoreConfig.rootPath + ClusterMapUtils.PARTITION_OVERRIDE_ZNODE_PATH;
    ZNRecord znRecord = new ZNRecord(ClusterMapUtils.PARTITION_OVERRIDE_STR);
    znRecord.setListField("AmbryList", list);
    if (!propertyStore.set(path, znRecord, AccessOption.PERSISTENT)) {
      fail("Failed to set HelixPropertyStore");
    }
    // Verify path exists
    assertTrue("The record path doesn't exist", propertyStore.exists(path, AccessOption.PERSISTENT));
    // Verify record
    ZNRecord result = propertyStore.get(path, null, AccessOption.PERSISTENT);
    assertEquals("Mismatch in list content", new HashSet<>(list), new HashSet<>(result.getListField("AmbryList")));
    zkInfo.shutdown();
  }
}
