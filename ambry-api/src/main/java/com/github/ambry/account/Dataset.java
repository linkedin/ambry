/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;


/**
 * <p>
 *  A dataset represents a logical data entity (e.g., a model, or an index, or some collection of directories). Like
 * accounts, a dataset needs to be on-boarded before any data can be stored in it, but unlike accounts which are on-boarded
 * via Nuage UI, datasets can be on-boarded by the account owner via rest request. A dataset has some metadata associated
 * with it, that defines either some characteristics of the dataset (e.g., what kind of versioning scheme would be used
 * to version that dataset) or some specification to Ambry as to how this dataset is to be handled by Ambry (e.g., delete
 * any version of this dataset that is more than N days old).
 * </p>
 * <p>
 *   Dataset name is provided by a user as an external reference to the dataset. Dataset name has to be distinct within
 *   the same {@link Account}, but can be the same across different {@link Account}s.
 * </p>
 * <pre><code>
 *  {
 *    "accountName": "MyPrivateAccount",
 *    "containerName": "MyPrivateContainer",
 *    "datasetName": "MyPrivateDataset",
 *    "versionSchema": "TIMESTAMP",
 *    "expirationTimeMs": -1,
 *    "retentionCount": 10,
 *    "retentionTimeInSeconds": 3600,
 *    "userTags": "{userTag1:tagValue1}"
 *  } * </code></pre>
 */
@JsonDeserialize(builder = DatasetBuilder.class)
public class Dataset {
  //constant

  /**
   * The pattern defining valid Dataset names in Ambry. Names must begin with an alphanumeric character, followed
   * by up to 100 characters that are alphanumeric, {@code _} , {@code .} , or {@code -} (underscore, period,
   * or hyphen).
   */
  private static final Pattern AMBRY_VALID_DATASET_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}");

  static final String ACCOUNT_NAME_KEY = "accountName";
  static final String CONTAINER_NAME_KEY = "containerName";
  static final String DATASET_NAME_KEY = "datasetName";
  static final String JSON_VERSION_SCHEMA_KEY = "versionSchema";
  static final String JSON_RETENTION_COUNT_KEY = "retentionCount";
  static final String JSON_RETENTION_TIME_KEY = "retentionTimeInSeconds";
  static final String JSON_USER_TAGS_KEY = "userTags";

  @JsonProperty(ACCOUNT_NAME_KEY)
  private final String accountName;
  @JsonProperty(CONTAINER_NAME_KEY)
  private final String containerName;
  @JsonProperty(DATASET_NAME_KEY)
  private final String datasetName;
  @JsonProperty(JSON_VERSION_SCHEMA_KEY)
  private final VersionSchema versionSchema;
  @JsonProperty(JSON_RETENTION_COUNT_KEY)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private final Integer retentionCount;
  @JsonProperty(JSON_RETENTION_TIME_KEY)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private final Long retentionTimeInSeconds;
  @JsonProperty(JSON_USER_TAGS_KEY)
  private final Map<String, String> userTags;

  /**
   * Constructor that takes individual arguments.
   * @param accountName The name of the account. Cannot be null.
   * @param containerName The name of the container. Cannot be null.
   * @param datasetName The name of the dataset. Cannot be null.
   * @param versionSchema The schema of the version. Cannot be null.
   * @param retentionCount The retention of dataset by count. The older versions will be deprecated. Can be null.
   * @param retentionTimeInSeconds The time-to-live for versions of this dataset. Numbers equals to -1 indicate an unlimited retention.
   * @param userTags The user defined metadata. Can be null.
   */
  public Dataset(String accountName, String containerName, String datasetName, VersionSchema versionSchema,
      Integer retentionCount, Long retentionTimeInSeconds, Map<String, String> userTags) {
    checkPreconditions(accountName, containerName, datasetName);
    this.accountName = accountName;
    this.containerName = containerName;
    this.datasetName = datasetName;
    this.versionSchema = versionSchema;
    this.retentionCount = retentionCount;
    this.retentionTimeInSeconds = retentionTimeInSeconds;
    this.userTags = userTags;
  }

  public enum VersionSchema {
    TIMESTAMP, MONOTONIC, SEMANTIC
  }

  /**
   * @return the account name.
   */
  @JsonProperty(ACCOUNT_NAME_KEY)
  public String getAccountName() {
    return accountName;
  }

  /**
   * @return the container name.
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * @return the dataset name.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * @return the version schema.
   */
  public VersionSchema getVersionSchema() {
    return versionSchema;
  }

  /**
   * @return the retention of dataset by count. The older versions will be deprecated.
   */
  public Integer getRetentionCount() {
    return retentionCount;
  }

  /**
   * @return the time-to-live for versions of this dataset.
   */
  public Long getRetentionTimeInSeconds() {return retentionTimeInSeconds; }

  /**
   * @return the user defined metadata.
   */
  @JsonProperty(JSON_USER_TAGS_KEY)
  public Map<String, String> getUserTags() {
    return userTags;
  }

  /**
   * Checks if any required fields is missing for a {@link Dataset} or for any incompatible settings.
   * @param accountName The name of the account. Cannot be null.
   * @param containerName The name of the container. Cannot be null.
   * @param datasetName The name of the dataset. Cannot be null.
   */
  private void checkPreconditions(String accountName, String containerName, String datasetName) {
    if (accountName == null || containerName == null || datasetName == null) {
      throw new IllegalStateException(
          "At lease one of required fields accountName=" + accountName + " or containerName=" + containerName
              + " or datasetName=" + datasetName + " is null");
    }
    if (accountName.isEmpty() || containerName.isEmpty() || datasetName.isEmpty()) {
      throw new IllegalArgumentException(
          "At lease one of required fields accountName=" + accountName + " or containerName=" + containerName
              + " or datasetName=" + datasetName + " is empty");
    }
    if (!AMBRY_VALID_DATASET_NAME_PATTERN.matcher(datasetName).matches()) {
      throw new IllegalArgumentException("Invalid name for an Ambry dataset: " + datasetName
          + ". Valid names should only include alphanumeric characters, periods, underscores, and hyphens. The exact regex you must match is: "
          + AMBRY_VALID_DATASET_NAME_PATTERN);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dataset dataset = (Dataset) o;
    return Objects.equals(accountName, dataset.accountName)
        && Objects.equals(containerName, dataset.containerName)
        && Objects.equals(datasetName, dataset.datasetName)
        && versionSchema == dataset.versionSchema
        && Objects.equals(retentionCount, dataset.retentionCount)
        && Objects.equals(retentionTimeInSeconds, dataset.retentionTimeInSeconds)
        && Objects.equals(userTags, dataset.userTags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountName, containerName, datasetName);
  }

  @Override
  public String toString() {
    return "Dataset[" + getAccountName() + ":" + getContainerName() + ":" + getDatasetName() + ":" + getVersionSchema()
        + "]";
  }
}
