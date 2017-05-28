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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
 *   Account is serialized into {@link JSONObject} in the highest metadata version, which is version 1 for now.
 *   Below lists all the metadata versions and their formats:
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
 * }
 * </code></pre>
 * <p>
 *   An account object is immutable. To update an account, refer to {@link AccountBuilder} for how to build a new
 *   account object with updated field(s).
 * </p>
 */
public class Account {
  // static variables
  static final String ACCOUNT_METADATA_VERSION_KEY = "version";
  static final String ACCOUNT_ID_KEY = "accountId";
  static final String ACCOUNT_NAME_KEY = "accountName";
  static final String ACCOUNT_STATUS_KEY = "status";
  static final String CONTAINERS_KEY = "containers";
  static final short ACCOUNT_METADATA_VERSION_1 = 1;
  static final short HIGHEST_ACCOUNT_METADATA_VERSION = ACCOUNT_METADATA_VERSION_1;
  // account member variables
  private final Short id;
  private final String name;
  private AccountStatus status;
  // internal data structure
  private final Map<Short, Container> containerIdToContainerMap = new HashMap<>();
  private final Map<String, Container> containerNameToContainerMap = new HashMap<>();

  /**
   * Constructing an {@link Account} object from account metadata.
   * @param metadata The metadata of the account in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  Account(JSONObject metadata) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    short metadataVersion = (short) metadata.getInt(ACCOUNT_METADATA_VERSION_KEY);
    switch (metadataVersion) {
      case ACCOUNT_METADATA_VERSION_1:
        id = (short) metadata.getInt(ACCOUNT_ID_KEY);
        name = metadata.getString(ACCOUNT_NAME_KEY);
        status = AccountStatus.valueOf(metadata.getString(ACCOUNT_STATUS_KEY));
        checkRequiredFieldsForBuild();
        JSONArray containerArray = metadata.getJSONArray(CONTAINERS_KEY);
        Collection<Container> containers = new ArrayList<>();
        if (containerArray != null) {
          for (int index = 0; index < containerArray.length(); index++) {
            JSONObject containerMetadata = containerArray.getJSONObject(index);
            Container container = new Container(containerMetadata);
            containers.add(container);
            updateContainerMap(container);
          }
          checkAccountIdInContainers();
          checkDuplicateContainerNameOrId(containers);
        }
        break;

      default:
        throw new IllegalStateException("Unsupported account metadata version=" + metadataVersion);
    }
  }

  /**
   * Constructor that takes individual arguments.
   * @param id The id of the account. Cannot be null.
   * @param name The name of the account. Cannot be null.
   * @param status The status of the account. Cannot be null.
   * @param containers A collection of {@link Container}s to be part of this account.
   */
  Account(Short id, String name, AccountStatus status, Collection<Container> containers) {
    this.id = id;
    this.name = name;
    this.status = status;
    checkRequiredFieldsForBuild();
    if (containers != null) {
      for (Container container : containers) {
        updateContainerMap(container);
      }
      checkAccountIdInContainers();
      checkDuplicateContainerNameOrId(containers);
    }
  }

  /**
   * Gets the id of the account.
   * @return The id of the account.
   */
  public short getId() {
    return id;
  }

  /**
   * Gets the name of the account.
   * @return The name of the account.
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the metadata of the account in {@link JSONObject}.
   * @return The metadata of the account in {@link JSONObject}.
   * @throws JSONException If fails to compose the metadata in {@link JSONObject}.
   */
  public JSONObject toJson() throws JSONException {
    JSONObject metadata = new JSONObject();
    metadata.put(ACCOUNT_METADATA_VERSION_KEY, HIGHEST_ACCOUNT_METADATA_VERSION);
    metadata.put(ACCOUNT_ID_KEY, id);
    metadata.put(ACCOUNT_NAME_KEY, name);
    metadata.put(ACCOUNT_STATUS_KEY, status);
    JSONArray containerArray = new JSONArray();
    for (Container container : containerIdToContainerMap.values()) {
      containerArray.put(container.toJson());
    }
    metadata.put(CONTAINERS_KEY, containerArray);
    return metadata;
  }

  /**
   * Gets the status of the account.
   * @return The status of the account.
   */
  public AccountStatus getStatus() {
    return status;
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
  public Collection<Container> getAllContainers() {
    return Collections.unmodifiableCollection(containerIdToContainerMap.values());
  }

  /**
   * Adds a {@link Container} to this account and updates internal maps accordingly.
   * @param container The container to update this account.
   */
  private void updateContainerMap(Container container) {
    containerIdToContainerMap.put(container.getId(), container);
    containerNameToContainerMap.put(container.getName(), container);
  }

  /**
   * Generates a String representation that uniquely identifies this account. The string is in the format of
   * {@code Account[id]}.
   * @return The String representation of this account.
   */
  @Override
  public String toString() {
    return "Account[" + getId() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Account)) {
      return false;
    }

    Account account = (Account) o;

    if (!id.equals(account.id)) {
      return false;
    }
    if (!name.equals(account.name)) {
      return false;
    }
    return status == account.status;
  }

  @Override
  public int hashCode() {
    return (int) id;
  }

  /**
   * The status of the account. {@code ACTIVE} means this account is in operational state, and {@code INACTIVE} means
   * the account has been deactivated.
   */
  public enum AccountStatus {
    ACTIVE, INACTIVE
  }

  /**
   * Checks if required fields are missing to build.
   */
  private void checkAccountIdInContainers() {
    for (Container container : containerIdToContainerMap.values()) {
      if (container.getParentAccountId() != id) {
        throw new IllegalStateException(
            "Container does not belong to this account because parentAccountId=" + container.getParentAccountId()
                + " is not the same as accountId=" + id);
      }
    }
  }

  /**
   * Checks if any required field is missing for a {@link Account}.
   */
  private void checkRequiredFieldsForBuild() {
    if (id == null || name == null || status == null) {
      throw new IllegalStateException(
          "Either of required fields id=" + id + " or name=" + name + " or status=" + status + " is null");
    }
  }

  /**
   * Checks if there are containers that have different ids but the same names, or vise versa.
   * @param containers A collection of containers to check duplicate name or id.
   * @throws IllegalStateException if there are containers that have different ids but the same names, or vise versa.
   */
  private void checkDuplicateContainerNameOrId(Collection<Container> containers) {
    int expectedKvPairs = containers == null ? 0 : containers.size();
    Set<Short> containerIdSet = containerIdToContainerMap.keySet();
    Set<String> containerNameSet = containerNameToContainerMap.keySet();
    if (containerIdSet.size() != expectedKvPairs || containerNameSet.size() != expectedKvPairs) {
      StringBuilder sb = new StringBuilder("Duplicate container id or name exists. Container list=");
      for (Container container : containers) {
        sb.append("[id=" + container.getId() + ", name=" + container.getName() + "] ");
      }
      throw new IllegalStateException(sb.toString());
    }
  }
}
