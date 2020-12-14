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
  public static final String QUOTA_CONFIG_PREFIX = "quota.";
  public static final String QUOTA_ENFORCER_FACTORIES = QUOTA_CONFIG_PREFIX + "enforcer.factories";
  public static final String STORAGE_QUOTA_SOURCE = QUOTA_CONFIG_PREFIX + "storage.quota.source";
  public static final String CAPACITY_UNIT_QUOTA_SOURCE = QUOTA_CONFIG_PREFIX + "capacity.unit.quota.source";
  public static final String HOST_RESOURCES_QUOTA_SOURCE = QUOTA_CONFIG_PREFIX + "host.resources.quota.source";
  public StorageQuotaConfig storageQuotaConfig;
  /**
   * {@link Set} of all the quota factory classes to be instantiated.
   */
  @Config(QUOTA_ENFORCER_FACTORIES)
  public Set<String> enabledQuotaEnforcers;

  /**
   * Class for storage quota source.
   */
  @Config(STORAGE_QUOTA_SOURCE)
  public String storageQuotaSource;

  /**
   * Class for capacity unit quota source.
   */
  @Config(CAPACITY_UNIT_QUOTA_SOURCE)
  public String capacityUnitQuotaSource;

  /**
   * Class for host resources quota source.
   */
  @Config(HOST_RESOURCES_QUOTA_SOURCE)
  public String hostResourcesQuotaSource;

  /**
   * Constructor for {@link QuotaConfig}.
   * @param verifiableProperties {@link VerifiableProperties} object.
   */
  public QuotaConfig(VerifiableProperties verifiableProperties) {
    storageQuotaConfig = new StorageQuotaConfig(verifiableProperties);
    String quotaEnforcers = verifiableProperties.getString(QUOTA_ENFORCER_FACTORIES);
    enabledQuotaEnforcers = Utils.splitString(quotaEnforcers, ",", HashSet::new);
    storageQuotaSource = verifiableProperties.getString(STORAGE_QUOTA_SOURCE, "");
    capacityUnitQuotaSource = verifiableProperties.getString(CAPACITY_UNIT_QUOTA_SOURCE, "");
    hostResourcesQuotaSource = verifiableProperties.getString(HOST_RESOURCES_QUOTA_SOURCE, "");
  }
}
