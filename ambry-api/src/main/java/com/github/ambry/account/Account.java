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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * <p>
 *   A representation of an Ambry user. A user is an entity (an application or an individual user) who uses ambry
 * as a service. This {@code Account} class contains general information of a user, which can be used for user-based
 * operations such as authentication, get a {@link Container} under this account, and access control.
 * </p>
 * <p>
 *   Account name is provided by an Ambry user as an external reference. Account id is an internal identifier
 *   of the user, and is one-to-one mapped to an account name. Account name and id are generated through user
 *   registration process.
 * </p>
 * <p>
 *   Account is serialized into {@link JSONObject} in the {@code currentJsonVersion}, which is version 1 for now.
 *   Below lists all the json versions and their formats:
 * </p>
 * <pre><code>
 * Version 1:
 * {
 *   "accountId": 101,
 *   "accountName": "MyAccount",
 *   "containers": [
 *     {
 *       "containerName": "MyPrivateContainer",
 *       "description": "This is my private container",
 *       "isPrivate": "true",
 *       "containerId": 0,
 *       "version": 1,
 *       "status": "ACTIVE",
 *       "parentAccountId": "101"
 *     },
 *     {
 *       "containerName": "MyPublicContainer",
 *       "description": "This is my public container",
 *       "isPrivate": "false",
 *       "containerId": 1,
 *       "version": 1,
 *       "status": "ACTIVE",
 *       "parentAccountId": "101"
 *     }
 *   ],
 *   "version": 1,
 *   "status": "ACTIVE"
 *   "quotaResourceType": "CONTAINER"
 * }
 * </code></pre>
 * <p>
 *   An account object is immutable. To update an account, refer to {@link AccountBuilder} for how to build a new
 *   account object with updated field(s).
 * </p>
 */
@JsonDeserialize(builder = AccountBuilder.class)
public class Account {
  /**
   * The id of unknown account.
   */
  public static final short UNKNOWN_ACCOUNT_ID = -1;
  /**
   * The name of the unknown account.
   */
  public static final String UNKNOWN_ACCOUNT_NAME = "ambry-unknown-account";
  /**
   * The id for to save account metadata in ambry.
   */
  public static final short HELIX_ACCOUNT_SERVICE_ACCOUNT_ID = -2;
  public static final short UNINITIALIZED_ACCOUNT_ID = -3;
  public static final String UNINITIALIZED_ACCOUNT_NAME = "__uninitialized_acount_name__";
  public static final QuotaResourceType QUOTA_RESOURCE_TYPE_DEFAULT_VALUE = QuotaResourceType.CONTAINER;
  // static variables
  static final String JSON_VERSION_KEY = "version";
  static final String ACCOUNT_ID_KEY = "accountId";
  static final String ACCOUNT_NAME_KEY = "accountName";
  static final String STATUS_KEY = "status";
  static final String SNAPSHOT_VERSION_KEY = "snapshotVersion";
  static final String CONTAINERS_KEY = "containers";
  static final String LAST_MODIFIED_TIME_KEY = "lastModifiedTime";
  static final String ACL_INHERITED_BY_CONTAINER_KEY = "aclInheritedByContainer";
  static final String QUOTA_RESOURCE_TYPE_KEY = "quotaResourceType";
  static final short JSON_VERSION_1 = 1;
  static final short CURRENT_JSON_VERSION = JSON_VERSION_1;
  static final int SNAPSHOT_VERSION_DEFAULT_VALUE = 0;
  static final long LAST_MODIFIED_TIME_DEFAULT_VALUE = 0;
  static final boolean ACL_INHERITED_BY_CONTAINER_DEFAULT_VALUE = false;

  // account member variables
  @JsonProperty(ACCOUNT_ID_KEY)
  private final short id;
  @JsonProperty(ACCOUNT_NAME_KEY)
  private final String name;
  private final int snapshotVersion;
  private final long lastModifiedTime;
  private final AccountStatus status;
  private final boolean aclInheritedByContainer;
  private final QuotaResourceType quotaResourceType;
  @JsonProperty(CONTAINERS_KEY)
  private final Set<Container> containers = new HashSet<>();
  @JsonProperty(JSON_VERSION_KEY)
  private final int version = CURRENT_JSON_VERSION;
  // internal data structure
  @JsonIgnore
  private final Map<Short, Container> containerIdToContainerMap = new HashMap<>();
  @JsonIgnore
  private final Map<String, Container> containerNameToContainerMap = new HashMap<>();

  /**
   * Constructing an {@link Account} object from account metadata.
   * @param metadata The metadata of the account in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  private Account(JSONObject metadata) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    short metadataVersion = (short) metadata.getInt(JSON_VERSION_KEY);
    switch (metadataVersion) {
      case JSON_VERSION_1:
        id = (short) metadata.getInt(ACCOUNT_ID_KEY);
        name = metadata.getString(ACCOUNT_NAME_KEY);
        status = AccountStatus.valueOf(metadata.getString(STATUS_KEY));
        snapshotVersion = metadata.optInt(SNAPSHOT_VERSION_KEY, SNAPSHOT_VERSION_DEFAULT_VALUE);
        lastModifiedTime = metadata.optLong(LAST_MODIFIED_TIME_KEY, LAST_MODIFIED_TIME_DEFAULT_VALUE);
        aclInheritedByContainer =
            metadata.optBoolean(ACL_INHERITED_BY_CONTAINER_KEY, ACL_INHERITED_BY_CONTAINER_DEFAULT_VALUE);
        checkRequiredFieldsForBuild();
        JSONArray containerArray = metadata.optJSONArray(CONTAINERS_KEY);
        if (containerArray != null) {
          List<Container> containers = new ArrayList<>();
          for (int index = 0; index < containerArray.length(); index++) {
            containers.add(Container.fromJson(containerArray.getJSONObject(index), id));
          }
          updateContainerMap(containers);
        }
        quotaResourceType =
            metadata.optEnum(QuotaResourceType.class, QUOTA_RESOURCE_TYPE_KEY, QUOTA_RESOURCE_TYPE_DEFAULT_VALUE);
        break;

      default:
        throw new IllegalStateException("Unsupported account json version=" + metadataVersion);
    }
  }

  /**
   * Constructor that takes individual arguments.
   * @param id The id of the account. Cannot be null.
   * @param name The name of the account. Cannot be null.
   * @param status The status of the account. Cannot be null.
   * @param aclInheritedByContainer Whether account's acl is inherited by container.
   * @param snapshotVersion the expected snapshot version for the account record.
   * @param containers A collection of {@link Container}s to be part of this account.
   * @param quotaResourceType {@link QuotaResourceType} on which quota will be enforced for this account.
   */
  Account(short id, String name, AccountStatus status, boolean aclInheritedByContainer, int snapshotVersion,
      Collection<Container> containers, QuotaResourceType quotaResourceType) {
    this(id, name, status, aclInheritedByContainer, snapshotVersion, containers, LAST_MODIFIED_TIME_DEFAULT_VALUE,
        quotaResourceType);
  }

  /**
   * Constructor that takes individual arguments.
   * @param id The id of the account. Cannot be null.
   * @param name The name of the account. Cannot be null.
   * @param status The status of the account. Cannot be null.
   * @param aclInheritedByContainer whether account's ACL is inherited by container.
   * @param snapshotVersion the expected snapshot version for the account record.
   * @param containers A collection of {@link Container}s to be part of this account.
   * @param lastModifiedTime created/modified time of this Account
   * @param quotaResourceType {@link QuotaResourceType} object.
   */
  Account(short id, String name, AccountStatus status, boolean aclInheritedByContainer, int snapshotVersion,
      Collection<Container> containers, long lastModifiedTime, QuotaResourceType quotaResourceType) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.snapshotVersion = snapshotVersion;
    this.lastModifiedTime = lastModifiedTime;
    this.aclInheritedByContainer = aclInheritedByContainer;
    checkRequiredFieldsForBuild();
    if (containers != null) {
      updateContainerMap(containers);
    }
    this.quotaResourceType = quotaResourceType;
  }

  /**
   * Deserializes a {@link JSONObject} to an account object.
   * @param json The {@link JSONObject} to deserialize.
   * @return An account object deserialized from the {@link JSONObject}.
   * @throws JSONException If parsing the {@link JSONObject} fails.
   */
  public static Account fromJson(JSONObject json) throws JSONException {
    return new Account(json);
  }

  /**
   * Gets the metadata of the account in {@link JSONObject}. This will increment the snapshot version by one if
   * {@code incrementSnapshotVersion} is {@code true}.
   * @param incrementSnapshotVersion {@code true} to increment the snapshot version by one.
   * @return The metadata of the account in {@link JSONObject}.
   * @throws JSONException If fails to compose the metadata in {@link JSONObject}.
   */
  public JSONObject toJson(boolean incrementSnapshotVersion) throws JSONException {
    JSONObject metadata = new JSONObject();
    metadata.put(JSON_VERSION_KEY, CURRENT_JSON_VERSION);
    metadata.put(ACCOUNT_ID_KEY, id);
    metadata.put(ACCOUNT_NAME_KEY, name);
    metadata.put(STATUS_KEY, status.name());
    metadata.put(SNAPSHOT_VERSION_KEY, incrementSnapshotVersion ? snapshotVersion + 1 : snapshotVersion);
    metadata.put(LAST_MODIFIED_TIME_KEY, lastModifiedTime);
    metadata.put(ACL_INHERITED_BY_CONTAINER_KEY, aclInheritedByContainer);
    JSONArray containerArray = new JSONArray();
    for (Container container : containerIdToContainerMap.values()) {
      containerArray.put(container.toJson());
    }
    metadata.put(CONTAINERS_KEY, containerArray);
    metadata.put(QUOTA_RESOURCE_TYPE_KEY, quotaResourceType.name());
    return metadata;
  }

  /**
   * @return The id of the account.
   */
  public short getId() {
    return id;
  }

  /**
   * @return The name of the account.
   */
  public String getName() {
    return name;
  }

  /**
   * @return The status of the account.
   */
  public AccountStatus getStatus() {
    return status;
  }

  /**
   * The snapshot version is generally the number of modifications to the account that were expected to have occurred
   * before the current time. This is used to validate that there were no unexpected account modifications that could be
   * inadvertently overwritten by an account update.
   * @return the expected version for the account record.
   */
  public int getSnapshotVersion() {
    return snapshotVersion;
  }

  /**
   * Get the created/modified time of this Account
   * @return epoch time in milliseconds
   */
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * @return whether account's ACL is inherited by containers.
   */
  public boolean isAclInheritedByContainer() {
    return aclInheritedByContainer;
  }

  /**
   * Gets the {@link Container} of this account with the specified container id.
   * @param containerId The id of the container to get.
   * @return The {@link Container} of this account with the specified id, or {@code null} if such a
   *                    container does not exist.
   */
  public Container getContainerById(short containerId) {
    return containerIdToContainerMap.get(containerId);
  }

  /**
   * Gets the {@link Container} of this account with the specified container name.
   * @param containerName The name of the container to get.
   * @return The {@link Container} of this account with the specified name, or {@code null} if such a
   *                    container does not exist.
   */
  public Container getContainerByName(String containerName) {
    return containerNameToContainerMap.get(containerName);
  }

  /**
   * Gets all the containers of this account in a Collection.
   * @return All the containers of this account.
   */
  @JsonIgnore
  public Collection<Container> getAllContainers() {
    return Collections.unmodifiableCollection(containerIdToContainerMap.values());
  }

  /**
   * @return the number of containers in this account.
   */
  @JsonIgnore
  public int getContainerCount() {
    return containerIdToContainerMap.size();
  }

  /**
   * @return QuotaResourceType.
   */
  public QuotaResourceType getQuotaResourceType() {
    return quotaResourceType;
  }

  /**
   * Generates a String representation that uniquely identifies this account. The string is in the format of
   * {@code Account[id]}.
   * @return The String representation of this account.
   */
  @Override
  public String toString() {
    return "Account[" + getId() + "," + getSnapshotVersion() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Account account = (Account) o;

    if (id != account.id) {
      return false;
    }
    if (!name.equals(account.name)) {
      return false;
    }
    if (status != account.status) {
      return false;
    }
    if (quotaResourceType != account.quotaResourceType) {
      return false;
    }
    return containerIdToContainerMap.equals(account.containerIdToContainerMap);
  }

  @Override
  public int hashCode() {
    return id;
  }

  /**
   * Adds a collection of {@link Container}(s) to this account and updates internal maps accordingly.
   * @param containers The containers to update this account.
   */
  public void updateContainerMap(Collection<Container> containers) {
    for (Container container : containers) {
      checkParentAccountIdInContainers(container);
      checkDuplicateContainerNameOrId(container);
      containerIdToContainerMap.put(container.getId(), container);
      containerNameToContainerMap.put(container.getName(), container);
      this.containers.add(container);
    }
  }

  /**
   * Checks if the parent account id in a {@link Container} matches the id of this account.
   * @param container The {@link Container} to check.
   * @throws IllegalStateException If the container's parentAccountId does not match the id of this account.
   */
  private void checkParentAccountIdInContainers(Container container) {
    if (container.getParentAccountId() != id) {
      throw new IllegalStateException(
          "Container does not belong to this account because parentAccountId=" + container.getParentAccountId()
              + " is not the same as accountId=" + id);
    }
  }

  /**
   * Checks if any required field is missing for a {@link Account}.
   * @throws IllegalStateException If any of the required field is missing.
   */
  private void checkRequiredFieldsForBuild() {
    if (name == null || status == null) {
      throw new IllegalStateException("Either of required fields name=" + name + " or status=" + status + " is null");
    }
  }

  /**
   * Checks a {@link Container}'s id or name conflicts with those in {@link #containerIdToContainerMap} or
   * {@link #containerNameToContainerMap}.
   * @param container A {@code Container} to check conflicting name or id.
   * @throws IllegalStateException If there are containers that have different ids but the same names, or vise versa.
   */
  private void checkDuplicateContainerNameOrId(Container container) {
    if (containerIdToContainerMap.containsKey(container.getId()) || containerNameToContainerMap.containsKey(
        container.getName())) {
      Container conflictContainer = containerIdToContainerMap.get(container.getId());
      conflictContainer =
          conflictContainer == null ? containerNameToContainerMap.get(container.getName()) : conflictContainer;
      String errorMessage =
          new StringBuilder("Duplicate container id or name exists. containerId=").append(container.getId())
              .append(" containerName=")
              .append(container.getName())
              .append(" conflicts with containerId=")
              .append(conflictContainer.getId())
              .append(" containerName=")
              .append(conflictContainer.getName())
              .toString();
      throw new IllegalStateException(errorMessage);
    }
  }

  /**
   * The status of the account. {@code ACTIVE} means this account is in operational state, and {@code INACTIVE} means
   * the account has been deactivated.
   */
  public enum AccountStatus {
    ACTIVE, INACTIVE
  }
}
