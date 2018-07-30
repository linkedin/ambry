package com.github.ambry.account;

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
}
