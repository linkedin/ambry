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
 * <p>
 * A representation of configuration for replicating an Ambry container to a cloud destination.
 * An Ambry container can have zero or more replication configs.
 * </p>
 * <p>
 *   CloudReplicationConfig is serialized into {@link JSONObject} in the following format:
 * </p>
 *  <pre><code>
 *  {
 *    "cloudDestinationType": "AZURE",
 *    "cloudConfigSpec": "Encrypted config spec for cloud service account",
 *    "cloudContainerName": "My container name"
 *  }
 *  </code></pre>
 *  <p>
 *    A CloudReplicationConfig object is immutable. To update a container, use {@link CloudReplicationConfig.Builder}.
 *  </p>
 */
public class CloudReplicationConfig {

  static final String CLOUD_DEST_TYPE_KEY = "cloudDestinationType";
  static final String CLOUD_CONFIG_SPEC_KEY = "cloudConfigSpec";
  static final String CLOUD_CONTAINER_NAME_KEY = "cloudContainerName";

  private String destinationType;
  // encrypted config string
  private String configSpec;
  private String cloudContainerName;

  private CloudReplicationConfig(String destinationType, String configSpec, String cloudContainerName) {
    this.destinationType = destinationType;
    this.configSpec = configSpec;
    this.cloudContainerName = cloudContainerName;
  }

  /**
   * @return the cloud destination type
   */
  public String getDestinationType() {
    return destinationType;
  }

  /**
   * @return the cloud configuration spec
   */
  public String getConfigSpec() {
    return configSpec;
  }

  /**
   * @return the cloud container name (optional property)
   */
  public String getCloudContainerName() {
    return cloudContainerName;
  }

  /**
   * Constructing a {@link CloudReplicationConfig} object from JSON metadata.
   * @param metadata The metadata in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  CloudReplicationConfig(JSONObject metadata) throws JSONException {
    destinationType = metadata.getString(CLOUD_DEST_TYPE_KEY);
    configSpec = metadata.getString(CLOUD_CONFIG_SPEC_KEY);
    cloudContainerName = metadata.optString(CLOUD_CONTAINER_NAME_KEY, null);
  }

  /**
   * @return The metadata of the replication config.
   * @throws JSONException If fails to compose metadata.
   */
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

  /**
   * Builder used to construct instances of {@link CloudReplicationConfig}
   */
  public static class Builder {
    // required
    private String destinationType;
    private String configSpec;

    // optional
    private String cloudContainerName;

    /**
     * Constructor to build a new {@link CloudReplicationConfig} from an existing {@link CloudReplicationConfig}.
     * The builder will include all the information of the existing {@link CloudReplicationConfig}.
     * This constructor should be used when modifying an existing config.
     * @param origin The {@link CloudReplicationConfig} to build from.
     */
    public Builder(CloudReplicationConfig origin) {
      this.destinationType = origin.destinationType;
      this.configSpec = origin.configSpec;
      this.cloudContainerName = origin.cloudContainerName;
    }

    /**
     * Constructor for a {@link CloudReplicationConfig} taking individual arguments.
     * @param destinationType The destination type of the {@link CloudReplicationConfig} to build.
     * @param configSpec The configuration spec of the {@link CloudReplicationConfig} to build.
     */
    public Builder(String destinationType, String configSpec) {
      this.destinationType = destinationType;
      this.configSpec = configSpec;
    }

    /**
     * Sets the destination type of the {@link CloudReplicationConfig} to build.
     * @param destinationType The destination type to set.
     * @return This builder.
     */
    public Builder setDestinationType(String destinationType) {
      this.destinationType = destinationType;
      return this;
    }

    /**
     * Sets the configuration spec of the {@link CloudReplicationConfig} to build.
     * @param configSpec The configuration spec to set.
     * @return This builder.
     */
    public Builder setConfigSpec(String configSpec) {
      this.configSpec = configSpec;
      return this;
    }

    /**
     * Sets the cloud container name of the {@link CloudReplicationConfig} to build.
     * @param cloudContainerName The cloud container name to set.
     * @return This builder.
     */
    public Builder setCloudContainerName(String cloudContainerName) {
      this.cloudContainerName = cloudContainerName;
      return this;
    }

    /**
     * @return the {@link CloudReplicationConfig} built from specified properties.
     */
    public CloudReplicationConfig build() {
      return new CloudReplicationConfig(destinationType, configSpec, cloudContainerName);
    }
  }
}
