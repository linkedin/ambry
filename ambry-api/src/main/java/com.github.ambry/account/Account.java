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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * A representation of an Ambry user. A user is an entity (an application or an individual user) who uses ambry
 * as service. This class contains general information of a user, which can be used for user-based operations
 * such as authentication, get a {@link Container} under this account, and access control. The account name is
 * provided by an Ambry user as an external reference. Account id is an internal identifier of the user, and is
 * one-to-one mapped to the account name. Account name and id are generated through user registration process.
 * Account id is part of a blob’s properties, and cannot be modified once the blob is created. Account metadata
 * is made in JSON, which is generic to contain additional information of the metadata.
 *
 * Version 1 of account metadata in JSON is in the format below.
 *  {
 *    "accountId": 101,
 *    "accountName": "MyAccount",
 *    "containers": [
 *      {
 *        "containerName": "MyPrivateContainer",
 *        "description": "This is my private container",
 *        "isPrivate": "true",
 *        "containerId": 0,
 *        "version": 1,
 *        "status": "ACTIVE",
 *        "parentAccountId": "101"
 *      },
 *      {
 *        "containerName": "MyPublicContainer",
 *        "description": "This is my public container",
 *        "isPrivate": "false",
 *        "containerId": 1,
 *        "version": 1,
 *        "status": "ACTIVE",
 *        "parentAccountId": "101"
 *      }
 *    ],
 *    "version": 1,
 *    "status": "ACTIVE"
 *  }
 *
 *  An account object is immutable. To update an account, refer to {@link AccountBuilder} to build a new account
 *  object with updated field(s).
 */
public class Account {
  // static variables
  static final String ACCOUNT_METADATA_VERSION_KEY = "version";
  static final String ACCOUNT_ID_KEY = "accountId";
  static final String ACCOUNT_NAME_KEY = "accountName";
  static final String ACCOUNT_STATUS_KEY = "status";
  static final String CONTAINERS_KEY = "containers";
  static final short ACCOUNT_METADATA_VERSION_1 = 1;
  // account member variables
  private final short version;
  private final short id;
  private final String name;
  private AccountStatus status;
  // internal data structure
  private final Map<Short, Container> containerIdToContainerMap = new HashMap<>();
  private final Map<String, Container> containerNameToContainerMap = new HashMap<>();

  /**
   * Constructor from account metadata.
   * @param metadata The metadata of the account in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  Account(JSONObject metadata) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    version = (short) metadata.getInt(ACCOUNT_METADATA_VERSION_KEY);
    switch (version) {
      case ACCOUNT_METADATA_VERSION_1:
        id = (short) metadata.getInt(ACCOUNT_ID_KEY);
        name = metadata.getString(ACCOUNT_NAME_KEY);
        status = AccountStatus.valueOf(metadata.getString(ACCOUNT_STATUS_KEY));
        JSONArray containerArray = metadata.getJSONArray(CONTAINERS_KEY);
        for (int index = 0; index < containerArray.length(); index++) {
          JSONObject containerMetadata = containerArray.getJSONObject(index);
          updateContainerMap(new Container(containerMetadata));
        }
        break;

      default:
        throw new IllegalStateException("Unsupported account metadata version=" + version);
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
    JSONObject metadata;
    switch (version) {
      case ACCOUNT_METADATA_VERSION_1:
        metadata = new JSONObject();
        metadata.put(ACCOUNT_METADATA_VERSION_KEY, version);
        metadata.put(ACCOUNT_ID_KEY, id);
        metadata.put(ACCOUNT_NAME_KEY, name);
        metadata.put(ACCOUNT_STATUS_KEY, status);
        JSONArray containerArray = new JSONArray();
        for (Container container : containerIdToContainerMap.values()) {
          containerArray.put(container.toJson());
        }
        metadata.put(CONTAINERS_KEY, containerArray);
        break;

      default:
        throw new IllegalArgumentException("Unsupported account metadata version=" + version);
    }
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
   * Gets all the containers of this account in a list.
   * @return All the containers of this account.
   */
  public List<Container> getAllContainers() {
    return Collections.unmodifiableList(new ArrayList<>(containerIdToContainerMap.values()));
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
   * Generates a {@link String} representation that uniquely identifies this account. The string
   * is in the format of {@code Account[id]}.
   * @return The {@link String} representation of this account.
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

    if (id != account.id) {
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
   * Status of the account. {@code ACTIVE} means this account is in operational state, and {@code INACTIVE} means
   * the account has been deactivated.
   */
  public enum AccountStatus {
    ACTIVE, INACTIVE
  }
}
