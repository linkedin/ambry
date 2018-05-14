/*
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
package com.github.ambry.account;

import static com.github.ambry.account.Container.*;


/**
 * A builder class for {@link Container}. Since {@link Container} is immutable, modifying a {@link Container} needs to
 * build a new {@link Container} object with updated fields through this builder. A {@link Container} can be built
 * in two ways: 1) from an existing {@link Container} object; and 2) by supplying required fields of a {@link Container}.
 * This class is not thread safe.
 */
public class ContainerBuilder {
  // necessary
  private short id;
  private String name;
  private ContainerStatus status;
  private String description;
  private short parentAccountId;

  // optional
  private boolean encrypted = ENCRYPTED_DEFAULT_VALUE;
  private boolean previouslyEncrypted = PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;
  private boolean cacheable = CACHEABLE_DEFAULT_VALUE;
  private boolean mediaScanDisabled = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;
  private String replicationPolicy = null;

  /**
   * Constructor. This will allow building a new {@link Container} from an existing {@link Container}. The builder will
   * include all the information of the existing {@link Container}. This constructor should be used when modifying an
   * existing container.
   * @param origin The {@link Container} to build from.
   */
  public ContainerBuilder(Container origin) {
    if (origin == null) {
      throw new IllegalArgumentException("origin cannot be null.");
    }
    id = origin.getId();
    name = origin.getName();
    status = origin.getStatus();
    description = origin.getDescription();
    encrypted = origin.isEncrypted();
    previouslyEncrypted = origin.wasPreviouslyEncrypted();
    cacheable = origin.isCacheable();
    mediaScanDisabled = origin.isMediaScanDisabled();
    replicationPolicy = origin.getReplicationPolicy();
    parentAccountId = origin.getParentAccountId();
  }

  /**
   * Constructor for a {@link ContainerBuilder} taking individual arguments.
   * @param id The id of the {@link Container} to build.
   * @param name The name of the {@link Container}.
   * @param status The status of the {@link Container}.
   * @param description The description of the {@link Container}.
   * @param parentAccountId The id of the parent {@link Account} of the {@link Container} to build.
   */
  public ContainerBuilder(short id, String name, ContainerStatus status, String description, short parentAccountId) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.parentAccountId = parentAccountId;
  }

  /**
   * Sets the ID of the {@link Container} to build.
   * @param id The ID to set.
   * @return This builder.
   */
  public ContainerBuilder setId(short id) {
    this.id = id;
    return this;
  }

  /**
   * Sets the name of the {@link Container} to build.
   * @param name The name to set.
   * @return This builder.
   */
  public ContainerBuilder setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Sets the status of the {@link Container} to build.
   * @param status The status to set.
   * @return This builder.
   */
  public ContainerBuilder setStatus(ContainerStatus status) {
    this.status = status;
    return this;
  }

  /**
   * Sets the description of the {@link Container} to build.
   * @param description The description to set.
   * @return This builder.
   */
  public ContainerBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  /**
   * Sets the ID of the parent {@link Account} of the {@link Container} to build.
   * @param parentAccountId The parent {@link Account} ID to set.
   * @return This builder.
   */
  public ContainerBuilder setParentAccountId(short parentAccountId) {
    this.parentAccountId = parentAccountId;
    return this;
  }

  /**
   * Sets the encryption setting of the {@link Container} to build.
   * @param encrypted The encryption setting to set.
   * @return This builder.
   */
  public ContainerBuilder setEncrypted(boolean encrypted) {
    this.encrypted = encrypted;
    return this;
  }

  /**
   * Sets the previously encrypted setting of the {@link Container} to build.
   * @param previouslyEncrypted The previouslyEncrypted setting to set.
   * @return This builder.
   */
  public ContainerBuilder setPreviouslyEncrypted(boolean previouslyEncrypted) {
    this.previouslyEncrypted = previouslyEncrypted;
    return this;
  }

  /**
   * Sets the caching setting of the {@link Container} to build
   * @param cacheable The cache setting to set.
   * @return This builder.
   */
  public ContainerBuilder setCacheable(boolean cacheable) {
    this.cacheable = cacheable;
    return this;
  }

  /**
   * Sets the media scan disabled setting of the {@link Container} to build
   * @param mediaScanDisabled The media scan disabled setting to set.
   * @return This builder.
   */
  public ContainerBuilder setMediaScanDisabled(boolean mediaScanDisabled) {
    this.mediaScanDisabled = mediaScanDisabled;
    return this;
  }

  /**
   * Sets the replication policy desired by the {@link Container}.
   * @param replicationPolicy the replication policy desired by the container
   * @return
   */
  public ContainerBuilder setReplicationPolicy(String replicationPolicy) {
    this.replicationPolicy = replicationPolicy;
    return this;
  }

  /**
   * Builds a {@link Container} object. {@code id}, {@code name}, {@code status}, {@code isPrivate}, and
   * {@code parentAccountId} are required before build.
   * @return A {@link Container} object.
   * @throws IllegalStateException If any required fields is not set.
   */
  public Container build() {
    return new Container(id, name, status, description, encrypted, previouslyEncrypted || encrypted, cacheable,
        mediaScanDisabled, replicationPolicy, parentAccountId);
  }
}
