/**
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
package com.github.ambry.account;

import java.util.Objects;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Configuration for replicating an Ambry container to a cloud destination.
 */
public class CloudReplicationConfig {

  static final String CLOUD_DEST_TYPE_KEY = "cloudDestinationType";
  static final String CLOUD_CONFIG_SPEC_KEY = "cloudConfigSpec";
  static final String CLOUD_CONTAINER_NAME_KEY = "cloudContainerName";

  private String destinationType;
  // encrypted config string
  private String configSpec;
  private String cloudContainerName;

  public CloudReplicationConfig(String destinationType, String configSpec, String cloudContainerName) {
    this.destinationType = destinationType;
    this.configSpec = configSpec;
    this.cloudContainerName = cloudContainerName;
  }

  public String getDestinationType() {
    return destinationType;
  }

  public String getConfigSpec() {
    return configSpec;
  }

  public String getCloudContainerName() {
    return cloudContainerName;
  }

  CloudReplicationConfig(JSONObject metadata) throws JSONException {
    destinationType = metadata.optString(CLOUD_DEST_TYPE_KEY, null);
    configSpec = metadata.getString(CLOUD_CONFIG_SPEC_KEY);
    cloudContainerName = metadata.optString(CLOUD_CONTAINER_NAME_KEY, null);
  }

  public JSONObject toJson() throws JSONException {
    JSONObject metadata = new JSONObject();
    metadata.putOpt(CLOUD_DEST_TYPE_KEY, destinationType);
    metadata.putOpt(CLOUD_CONFIG_SPEC_KEY, configSpec);
    metadata.putOpt(CLOUD_CONTAINER_NAME_KEY, cloudContainerName);
    return metadata;
  }

  @Override
  public int hashCode() {
    return (destinationType + configSpec + cloudContainerName).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CloudReplicationConfig)) {
      return false;
    }
    CloudReplicationConfig oconfig = (CloudReplicationConfig) o;
    return (Objects.equals(destinationType, oconfig.destinationType)
        && Objects.equals(configSpec, oconfig.configSpec)
        && Objects.equals(cloudContainerName, oconfig.cloudContainerName));
  }
}
