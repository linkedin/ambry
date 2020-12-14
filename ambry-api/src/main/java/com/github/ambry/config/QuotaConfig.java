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
 */
package com.github.ambry.config;

import com.github.ambry.utils.Utils;
import java.util.HashSet;
import java.util.Set;


/**
 * Config for quota of various resources in Ambry.
 */
public class QuotaConfig {
  public static final String QUOTA_ENFORCER_FACTORIES = "quota.enforcer.factories";
  public StorageQuotaConfig storageQuotaConfig;

  /**
   * {@link Set} of all the quota factory classes to be instantiated.
   */
  @Config(QUOTA_ENFORCER_FACTORIES)
  public Set<String> enabledQuotaEnforcers;

  /**
   * Constructor for {@link QuotaConfig}.
   * @param verifiableProperties {@link VerifiableProperties} object.
   */
  public QuotaConfig(VerifiableProperties verifiableProperties) {
    storageQuotaConfig = new StorageQuotaConfig(verifiableProperties);
    String quotaThrottlers = verifiableProperties.getString("replication.vcr.recovery.partitions", "");
    enabledQuotaEnforcers = Utils.splitString(quotaThrottlers, ",", HashSet::new);
  }
}
