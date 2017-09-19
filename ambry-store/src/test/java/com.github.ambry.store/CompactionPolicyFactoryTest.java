/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Tests {@link CompactionPolicyFactory}
 */
public class CompactionPolicyFactoryTest {

  /**
   * Tests {@link CompactionPolicyFactory}
   * @throws Exception
   */
  @Test
  public void testCompactionPolicyFactory() throws Exception {
    List<Pair<String, String>> validCompactionPolicyInfos = new ArrayList<>();
    validCompactionPolicyInfos.add(new Pair<>("com.github.ambry.store.StatsBasedCompactionPolicyFactory",
        "com.github.ambry.store.StatsBasedCompactionPolicy"));
    validCompactionPolicyInfos.add(
        new Pair<>("com.github.ambry.store.CompactAllPolicyFactory", "com.github.ambry.store.CompactAllPolicy"));
    for (Pair<String, String> validCompactionPolicyInfo : validCompactionPolicyInfos) {
      Properties properties = new Properties();
      properties.setProperty("store.compaction.policy.factory", validCompactionPolicyInfo.getFirst());
      StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
      Time time = new MockTime();
      CompactionPolicyFactory compactionPolicyFactory = Utils.getObj(config.storeCompactionPolicyFactory, config, time);
      Assert.assertEquals("Did not receive expected CompactionPolicy instance", validCompactionPolicyInfo.getFirst(),
          compactionPolicyFactory.getClass().getCanonicalName());
      CompactionPolicy compactionPolicy = compactionPolicyFactory.getCompactionPolicy();
      Assert.assertEquals("Did not receive expected CompactionPolicy instance", validCompactionPolicyInfo.getSecond(),
          compactionPolicy.getClass().getCanonicalName());
    }
  }
}
