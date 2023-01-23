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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

import static com.github.ambry.account.Dataset.*;


/**
 * A builder class for {@link Dataset}. Modifying a {@link Dataset} needs to build a new {@link Dataset} object with
 * updated fields through this builder. A {@link Dataset} can be built in two ways: 1) from an existing {@link Dataset}
 * object; and 2) by supplying required fields of a {@link Dataset}.
 */
public class DatasetBuilder {
  // necessary
  private String accountName;
  private String containerName;
  private String datasetName;
  private Dataset.VersionSchema versionSchema;
  private long expirationTimeMs;
  // optional
  private Integer retentionCount = RETENTION_COUNT_DEFAULT;
  private Map<String, String> userTags = USER_TAGS_DEFAULT;

  /**
   * Constructor for jackson to deserialize {@link Dataset}.
   */
  public DatasetBuilder() {
  }

  /**
   * Constructor. This will allow building a new {@link Dataset} from an existing {@link Dataset}.
   * The builder will include all the information of the existing {@link Dataset}.
   * This constructor should be used when modifying an existing dataset.
   * @param origin The {@link Dataset} to build from.
   */
  public DatasetBuilder(Dataset origin) {
    if (origin == null) {
      throw new IllegalArgumentException("origin cannot be null.");
    }
    accountName = origin.getAccountName();
    containerName = origin.getContainerName();
    datasetName = origin.getDatasetName();
    versionSchema = origin.getVersionSchema();
    expirationTimeMs = origin.getExpirationTimeMs();
    retentionCount = origin.getRetentionCount();
    userTags = origin.getUserTags();
  }

  /**
   * Constructor for a {@link DatasetBuilder} taking individual arguments.
   * @param accountName The name of the account. Cannot be null.
   * @param containerName The name of the container. Cannot be null.
   * @param datasetName The name of the dataset. Cannot be null.
   * @param versionSchema The schema of the version. Cannot be null.
   * @param expirationTimeMs The expiration time in milliseconds since epoch, or -1 if the dataset should be permanent.
   *                         Cannot be null.
   */
  public DatasetBuilder(String accountName, String containerName, String datasetName, VersionSchema versionSchema,
      long expirationTimeMs) {
    this.accountName = accountName;
    this.containerName = containerName;
    this.datasetName = datasetName;
    this.versionSchema = versionSchema;
    this.expirationTimeMs = expirationTimeMs;
  }

  /**
   * Set the account name of the {@link Dataset} to build.
   * @param accountName the account name to set.
   * @return the builder.
   */
  @JsonProperty(ACCOUNT_NAME_KEY)
  public DatasetBuilder setAccountName(String accountName) {
    this.accountName = accountName;
    return this;
  }

  /**
   * Set the container name of the {@link Dataset} to build.
   * @param containerName the container name to set.
   * @return the builder.
   */
  @JsonProperty(CONTAINER_NAME_KEY)
  public DatasetBuilder setContainerName(String containerName) {
    this.containerName = containerName;
    return this;
  }

  /**
   * Set the dataset name of the {@link Dataset} to build.
   * @param datasetName the dataset name to set.
   * @return the builder.
   */
  @JsonProperty(DATASET_NAME_KEY)
  public DatasetBuilder setDatasetName(String datasetName) {
    this.datasetName = datasetName;
    return this;
  }

  /**
   * Set the version schema of the {@link Dataset} to build.
   * @param versionSchema the version schema to set.
   * @return the builder.
   */
  @JsonProperty(JSON_VERSION_SCHEMA_KEY)
  public DatasetBuilder setVersionSchema(Dataset.VersionSchema versionSchema) {
    this.versionSchema = versionSchema;
    return this;
  }

  /**
   * Set the expiration time in milliseconds of the {@link Dataset} to build.
   * @param expirationTimeMs the expirationTime in milliseconds to set.
   * @return the builder.
   */
  @JsonProperty(JSON_EXPIRATION_TIME_KEY)
  public DatasetBuilder setExpirationTimeMs(long expirationTimeMs) {
    this.expirationTimeMs = expirationTimeMs;
    return this;
  }

  /**
   * Set the retention count of the {@link Dataset} to build.
   * @param retentionCount the retention count to set.
   * @return the builder.
   */
  @JsonProperty(JSON_RETENTION_COUNT_KEY)
  public DatasetBuilder setRetentionCount(int retentionCount) {
    this.retentionCount = retentionCount;
    return this;
  }

  /**
   * Set the user tag of the {@link Dataset} to build.
   * @param userTags the user tags to set.
   * @return the builder.
   */
  @JsonProperty(JSON_USER_TAGS_KEY)
  public DatasetBuilder setUserTags(Map<String, String> userTags) {
    this.userTags = userTags;
    return this;
  }

  /**
   * Build the {@link Dataset} object. The accountName, containerName, datasetName, versionSchema and expirationTimeMs
   * are required before build.
   * @return a {@link Dataset} object.
   */
  public Dataset build() {
    return new Dataset(accountName, containerName, datasetName, versionSchema, expirationTimeMs, retentionCount,
        userTags);
  }
}
