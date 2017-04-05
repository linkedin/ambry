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
package com.github.ambry.rest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;


/**
 * A representation of an Ambry account. An account is an internal representation of a user.
 */
public class Account {
  private final short id;
  private final String userId;
  private final JSONObject metadata;
  private final List<Container> containers;
  private final Map<Short, Container> idContainerMap;
  private final Map<String, Container> nameContainerMap;

  /**
   * Constructor of Account.
   * @param id The id of the account.
   * @param userId The id of the corresponding user.
   * @param metadata The metadata of the account in JSON.
   * @param containers The list of containers of the account.
   */
  public Account(short id, String userId, JSONObject metadata, List<Container> containers) {
    if (userId == null) {
      throw new IllegalArgumentException("userId cannot be null");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null");
    }
    if (containers == null) {
      throw new IllegalArgumentException("containers cannot be null");
    }
    this.id = id;
    this.userId = userId;
    this.metadata = metadata;
    this.containers = containers;
    idContainerMap = new HashMap<>();
    nameContainerMap = new HashMap<>();
    for (Container container : containers) {
      idContainerMap.put(container.id(), container);
      nameContainerMap.put(container.name(), container);
    }
  }

  /**
   * Gets the id of the account.
   * @return The account id of the account.
   */
  public short id() {
    return id;
  }

  /**
   * Gets the user id of the account.
   * @return The user id of the account.
   */
  public String userId() {
    return userId;
  }

  /**
   * Gets the metadata of the account in JSON format.
   * @return The metadata of the account in JSON format.
   */
  public JSONObject metadata() {
    return metadata;
  }

  /**
   * Gets the {@link Container} of this account with the specified id.
   * @param containerId The id of the container to get.
   * @return The {@link Container} of this account with he specified id, or {@code null} if such a
   *                    container does not exist.
   */
  public Container getContainerById(short containerId) {
    return idContainerMap.get(containerId);
  }

  /**
   * Gets the {@link Container} of this account with the specified name.
   * @param containerName The name of the container to get.
   * @return The {@link Container} of this account with he specified name, or {@code null} if such a
   *                    container does not exist.
   */
  public Container getContainerByName(String containerName) {
    return nameContainerMap.get(containerName);
  }

  /**
   * Gets all the containers of this account.
   * @return All the containers of this account.
   */
  public List<Container> getAllContainers() {
    return Collections.unmodifiableList(containers);
  }
}
