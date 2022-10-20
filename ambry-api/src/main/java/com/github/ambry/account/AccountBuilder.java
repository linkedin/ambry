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

import com.github.ambry.quota.QuotaResourceType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.github.ambry.account.Account.*;


/**
 * A builder class for {@link Account}. Since {@link Account} is immutable, modifying an {@link Account} needs to
 * build a new {@link Account} object with updated fields through this builder. An {@link Account} can be built
 * in two ways: 1) from an existing {@link Account} object; and 2) by supplying required fields of an {@link Account}.
 * This class is not thread safe.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPOJOBuilder(withPrefix = "")
public class AccountBuilder {
  private Short id = null;
  private String name = null;
  private AccountStatus status = null;
  private QuotaResourceType quotaResourceType = QUOTA_RESOURCE_TYPE_DEFAULT_VALUE;
  private int snapshotVersion = SNAPSHOT_VERSION_DEFAULT_VALUE;
  private long lastModifiedTime = LAST_MODIFIED_TIME_DEFAULT_VALUE;
  private boolean aclInheritedByContainer = ACL_INHERITED_BY_CONTAINER_DEFAULT_VALUE;
  private final Map<Short, Container> idToContainerMetadataMap = new HashMap<>();

  /**
   * Constructor. This will build a new {@link Account} from an existing {@link Account} object. The builder will
   * include all the information including the {@link Container}s of the existing {@link Account}.
   * @param origin The {@link Account} to build from.
   */
  public AccountBuilder(Account origin) {
    if (origin == null) {
      throw new IllegalArgumentException("origin cannot be null.");
    }
    id = origin.getId();
    name = origin.getName();
    status = origin.getStatus();
    aclInheritedByContainer = origin.isAclInheritedByContainer();
    snapshotVersion = origin.getSnapshotVersion();
    lastModifiedTime = origin.getLastModifiedTime();
    for (Container container : origin.getAllContainers()) {
      idToContainerMetadataMap.put(container.getId(), container);
    }
    quotaResourceType = origin.getQuotaResourceType();
  }

  /**
   * Constructor for jackson to deserialize {@link Account}.
   */
  public AccountBuilder() {
  }

  /**
   * Constructor. The builder will not include any {@link Container} information.
   * @param id The id of the {@link Account} to build. Can be {@code null}, but should be set before
   *           calling {@link #build()}.
   * @param name The name of the {@link Account}. Can be {@code null}, but should be set before
   *           calling {@link #build()}.
   * @param status The status of the {@link Account}. Can be {@code null}, but should be set before
   *           calling {@link #build()}.
   */
  public AccountBuilder(short id, String name, AccountStatus status) {
    this(id, name, status, QUOTA_RESOURCE_TYPE_DEFAULT_VALUE);
  }

  /**
   * Constructor. The builder will not include any {@link Container} information.
   * @param id The id of the {@link Account} to build. Can be {@code null}, but should be set before
   *           calling {@link #build()}.
   * @param name The name of the {@link Account}. Can be {@code null}, but should be set before
   *           calling {@link #build()}.
   * @param status The status of the {@link Account}. Can be {@code null}, but should be set before
   *           calling {@link #build()}.
   * @param quotaResourceType The {@link QuotaResourceType} object for which quota enforcement will happen in this account.
   */
  public AccountBuilder(short id, String name, AccountStatus status, QuotaResourceType quotaResourceType) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.quotaResourceType = quotaResourceType;
  }

  /**
   * Sets the id of the {@link Account} to build.
   * @param id The id to set.
   * @return This builder.
   */
  @JsonProperty(ACCOUNT_ID_KEY)
  public AccountBuilder id(short id) {
    this.id = id;
    return this;
  }

  /**
   * Sets the name of the {@link Account} to build.
   * @param name The name to set.
   * @return This builder.
   */
  @JsonProperty(ACCOUNT_NAME_KEY)
  public AccountBuilder name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Sets the status of the {@link Account} to build.
   * @param status The id to set.
   * @return This builder.
   */
  public AccountBuilder status(AccountStatus status) {
    this.status = status;
    return this;
  }

  /**
   * Sets the snapshot version of the {@link Account} to build.
   * @param snapshotVersion The version to set.
   * @return This builder.
   */
  public AccountBuilder snapshotVersion(int snapshotVersion) {
    this.snapshotVersion = snapshotVersion;
    return this;
  }

  /**
   * Sets the created/modified time of the {@link Account} to build.
   * @param lastModifiedTime time in milliseconds.
   * @return This builder.
   */
  public AccountBuilder lastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
    return this;
  }

  /**
   * Specifies whether acl of {@link Account} is inherited by {@link Container}.
   * @param aclInheritedByContainer whether account's ACL is inherited by container.
   * @return This builder.
   */
  public AccountBuilder aclInheritedByContainer(boolean aclInheritedByContainer) {
    this.aclInheritedByContainer = aclInheritedByContainer;
    return this;
  }

  /**
   * Specifies the {@link QuotaResourceType} for the account.
   * @param quotaResourceType {@link QuotaResourceType} for the account.
   * @return This builder.
   */
  public AccountBuilder quotaResourceType(QuotaResourceType quotaResourceType) {
    this.quotaResourceType = quotaResourceType;
    return this;
  }

  /**
   * Clear the set of containers for the {@link Account} to build and add the provided ones.
   * @param containers A collection of {@link Container}s to use. Can be {@code null} to just remove all containers.
   * @return This builder.
   */
  public AccountBuilder containers(Collection<Container> containers) {
    idToContainerMetadataMap.clear();
    if (containers != null) {
      for (Container container : containers) {
        idToContainerMetadataMap.put(container.getId(), container);
      }
    }
    return this;
  }

  /**
   * Adds a {@link Container} for the {@link Account} to build. If the builder already has a {@link Container} with
   * the same id as new {@link Container} to set, the new {@link Container} will replace the existing {@link Container}.
   * @param container The new {@link Container} to set.
   * @return This builder.
   */
  public AccountBuilder addOrUpdateContainer(Container container) {
    if (container != null) {
      idToContainerMetadataMap.put(container.getId(), container);
    }
    return this;
  }

  /**
   * Removes a {@link Container} in this builder, so that an {@link Account} to build will not have this
   * {@link Container}. It will be a no-op if no {@link Container} with the id exists in this builder.
   * @param container The {@link Container} to remove.
   * @return This builder.
   */
  public AccountBuilder removeContainer(Container container) {
    if (container != null) {
      short id = container.getId();
      Container containerToRemove = idToContainerMetadataMap.get(id);
      if (container.equals(containerToRemove)) {
        idToContainerMetadataMap.remove(id);
      }
    }
    return this;
  }

  /**
   * Builds an {@link Account} object. {@code id}, {@code name}, {@code status}, {@code lastModifiedTime} and
   * {@code containers} (if any) must be set before building.
   * @return An {@link Account} object.
   * @throws IllegalStateException If any required fields is not set or there is inconsistency in containers.
   */
  public Account build() {
    if (id == null) {
      throw new IllegalStateException("Account id is not present");
    }
    // Did we check the container parent account id here?
    for (Map.Entry<Short, Container> entry : idToContainerMetadataMap.entrySet()) {
      if (entry.getValue().getParentAccountId() == Container.UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID) {
        entry.setValue(new ContainerBuilder(entry.getValue()).setParentAccountId(id).build());
      } else {
        entry.setValue(new ContainerBuilder(entry.getValue()).build());
      }
    }
    return new Account(id, name, status, aclInheritedByContainer, snapshotVersion, idToContainerMetadataMap.values(),
        lastModifiedTime, quotaResourceType);
  }
}
