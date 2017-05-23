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
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
 *        "isPublic": "true",
 *        "containerId": 0,
 *        "version": 1,
 *        "status": "ACTIVE"
 *      },
 *      {
 *        "containerName": "MyPublicContainer",
 *        "description": "This is my public container",
 *        "acl": "false",
 *        "containerId": 1,
 *        "version": 1,
 *        "status": "ACTIVE"
 *      }
 *    ],
 *    "version": 1,
 *    "status": "ACTIVE"
 *  }
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
  private JSONObject metadata;
  // internal data structure
  private final Map<Short, Container> containerIdToContainerMap = new HashMap<>();
  private final Map<String, Container> containerNameToContainerMap = new HashMap<>();
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  /**
   * Constructor from account metadata.
   * @param metadata The metadata of the account in JSON.
   */
  public Account(JSONObject metadata) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    this.version = (short) metadata.getInt(ACCOUNT_METADATA_VERSION_KEY);
    switch (version) {
      case ACCOUNT_METADATA_VERSION_1:
        this.id = (short) metadata.getInt(ACCOUNT_ID_KEY);
        this.name = metadata.getString(ACCOUNT_NAME_KEY);
        this.status = AccountStatus.valueOf(metadata.getString(ACCOUNT_STATUS_KEY));
        this.metadata = metadata;
        JSONArray containerArray = metadata.getJSONArray(CONTAINERS_KEY);
        for (int index = 0; index < containerArray.length(); index++) {
          JSONObject containerMetadata = containerArray.getJSONObject(index);
          updateContainerMap(new Container(containerMetadata, this));
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported account metadata version=" + version);
    }
  }

  /**
   * Constructor of Account without any {@link Container}.
   * @param id The id of the account.
   * @param name The name of the account.
   * @param status The status of the account.
   * @throws JSONException
   */
  public Account(short id, String name, AccountStatus status) throws JSONException {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null.");
    }
    this.version = ACCOUNT_METADATA_VERSION_1;
    this.id = id;
    this.name = name;
    this.status = status;
    JSONObject tempMetadata = new JSONObject();
    tempMetadata.put(ACCOUNT_METADATA_VERSION_KEY, version);
    tempMetadata.put(ACCOUNT_ID_KEY, id);
    tempMetadata.put(ACCOUNT_NAME_KEY, name);
    tempMetadata.put(ACCOUNT_STATUS_KEY, status);
    tempMetadata.put(CONTAINERS_KEY, new JSONArray());
    metadata = tempMetadata;
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
   * Gets the metadata of the account in JSON format.
   * @return The metadata of the account.
   */
  public JSONObject getMetadata() {
    readWriteLock.readLock().lock();
    try {
      return metadata;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Gets the status of the account.
   * @return The status of the account.
   */
  public AccountStatus getStatus() {
    readWriteLock.readLock().lock();
    try {
      return status;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Sets the status of the account.
   * @param status The new status of the account.
   */
  public void setStatus(AccountStatus status) throws JSONException {
    readWriteLock.writeLock().lock();
    try {
      metadata.put(ACCOUNT_STATUS_KEY, status);
      this.status = status;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Gets the {@link Container} of this account with the specified container id.
   * @param containerId The id of the container to get.
   * @return The {@link Container} of this account with the specified id, or {@code null} if such a
   *                    container does not exist.
   */
  public Container getContainerByContainerId(short containerId) {
    readWriteLock.readLock().lock();
    try {
      return containerIdToContainerMap.get(containerId);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Gets the {@link Container} of this account with the specified container name.
   * @param containerName The name of the container to get.
   * @return The {@link Container} of this account with the specified name, or {@code null} if such a
   *                    container does not exist.
   */
  public Container getContainerByContainerName(String containerName) {
    readWriteLock.readLock().lock();
    try {
      return containerNameToContainerMap.get(containerName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Gets all the containers of this account in a list.
   * @return All the containers of this account.
   */
  public List<Container> getAllContainers() {
    readWriteLock.readLock().lock();
    try {
      return Collections.unmodifiableList(new ArrayList<>(containerIdToContainerMap.values()));
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Adds a container to this account without modifying the metadata of the account.
   * @param container The container to this account.
   */
  private void updateContainerMap(Container container) {
    containerIdToContainerMap.put(container.getId(), container);
    containerNameToContainerMap.put(container.getName(), container);
  }

  /**
   * Updates a {@link Container} and the corresponding container field in the account metadata. If the container does
   * not exist, the container will be added to the account and its account metadata.
   * @param container The container to update.
   * @throws JSONException
   */
  public void updateContainer(Container container) throws JSONException {
    if (container == null) {
      throw new IllegalArgumentException("Container cannot be null.");
    }
    JSONObject newMetadata = new JSONObject(metadata, JSONObject.getNames(metadata));
    switch (version) {
      case ACCOUNT_METADATA_VERSION_1:
        JSONArray newContainerArray = newMetadata.getJSONArray(CONTAINERS_KEY);
        for (int index = 0; index < newContainerArray.length(); index++) {
          JSONObject newContainerMetadata = newContainerArray.getJSONObject(index);
          if (newContainerMetadata.getInt(Container.CONTAINER_ID_KEY) == container.getId()) {
            newContainerArray.put(index, container.getMetadata());
            break;
          }
        }
        newContainerArray.put(container.getMetadata());
        readWriteLock.writeLock().lock();
        try {
          metadata = newMetadata;
          updateContainerMap(container);
        } finally {
          readWriteLock.writeLock().unlock();
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported account metadata version=" + version);
    }
  }

  /**
   * Generates a {@link String} representation that uniquely identifies this account. The string
   * is in the format of {@code Account[id:name]}.
   * @return The {@link String} representation of this account.
   */
  @Override
  public String toString() {
    return "Account[" + getId() + ":" + getName() + "]";
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
    return this.getStatus().equals(account.getStatus());
  }

  @Override
  public int hashCode() {
    return (int) id;
  }

  /**
   * Status of the account. {@code ACTIVE} means this account is in operational state, and {@code INACTIVE} means
   * the account is inactive, so accessing any container of this account will be denied.
   */
  public enum AccountStatus {
    ACTIVE, INACTIVE
  }
}
