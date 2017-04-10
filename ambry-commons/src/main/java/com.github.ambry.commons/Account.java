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
package com.github.ambry.commons;

import java.util.ArrayList;
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
  private final String name;
  private final JSONObject metadata;
  private final Map<Short, Container> idContainerMap;
  private final Map<String, Container> nameContainerMap;

  /**
   * Constructor of Account.
   * @param id The id of the account, which is the internal reference to the account.
   * @param name The name of the account, which is the external reference to the account.
   * @param metadata The metadata of the account in JSON.
   */
  public Account(short id, String name, JSONObject metadata) {
    if (name == null) {
      throw new IllegalArgumentException("accountName cannot be null");
    }
    this.id = id;
    this.name = name;
    this.metadata = metadata;
    idContainerMap = new HashMap<>();
    nameContainerMap = new HashMap<>();
  }

  /**
   * Gets the id of the account.
   * @return The id of the account.
   */
  public short id() {
    return id;
  }

  /**
   * Gets the name of the account.
   * @return The name of the account.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the metadata of the account in JSON format.
   * @return The metadata of the account in JSON format.
   */
  public JSONObject metadata() {
    return metadata;
  }

  /**
   * Gets the {@link Container} of this account with the specified container id.
   * @param containerId The id of the container to get.
   * @return The {@link Container} of this account with the specified id, or {@code null} if such a
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
   * Gets all the containers of this account in a list.
   * @return All the containers of this account.
   */
  public List<Container> getAllContainers() {
    return Collections.unmodifiableList(new ArrayList<>(idContainerMap.values()));
  }

  /**
   * Adds a container to this account.
   * @param container The container to this account.
   */
  void addContainer(Container container) {
    idContainerMap.put(container.id(), container);
    nameContainerMap.put(container.name(), container);
  }

  /**
   * Generates a {@link String} representation that uniquely identifies this account. The string
   * is in the format of {@code Account[id:name]}.
   * @return The {@link String} representation of this account.
   */
  @Override
  public String toString() {
    return "Account[" + id() + ":" + name() + "]";
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

    return id == account.id;
  }

  @Override
  public int hashCode() {
    return (int) id;
  }
}
