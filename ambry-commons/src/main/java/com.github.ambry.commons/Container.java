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

import org.json.JSONObject;


/**
 * A representation of a container. A container virtually groups a number of blobs under the same {@link Account},
 * so that an operation on the container will be applied to all the blobs of the container. There can be multiple
 * containers under the the same {@link Account}, but containers cannot be nested. Eventually, one and only one
 * container needs to be specified when posting a blob. Container id is part of the blob’s properties, and cannot
 * be modified once the blob is created.
 *
 * Container name is provided by a user as an external reference to that container under an {@link Account}. Container
 * id is one-to-one mapped to container name, and serves as an internal identifier of a container. Container name and
 * id name space of different accounts do not conflict. Container metadata is made in JSON, which is generic to contain
 * additional information of the metadata.
 */
public class Container {
  private final short id;
  private final String name;
  private final JSONObject metadata;
  private final Account parentAccount;

  /**
   * Constructor.
   * @param id The id of the container.
   * @param name The name of the container.
   * @param metadata The metadata of the container in JSON. Can be {@code null}.
   * @param parentAccount The parent {@link Account} of the container.
   */
  Container(short id, String name, JSONObject metadata, Account parentAccount) {
    if (name == null) {
      throw new IllegalArgumentException("containerName cannot be null");
    }
    if (parentAccount == null) {
      throw new IllegalArgumentException("parentAccount cannot be null");
    }
    this.id = id;
    this.name = name;
    this.metadata = metadata;
    this.parentAccount = parentAccount;
  }

  /**
   * Gets the id of the container.
   * @return The id of the container.
   */
  public short id() {
    return id;
  }

  /**
   * Gets the name of the container.
   * @return The name of the container.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the metadata of the container in JSON format.
   * @return The metadata of the container in JSON format.
   */
  public JSONObject metadata() {
    return metadata;
  }

  /**
   * Gets the parent {@link Account} of the container.
   * @return The parent {@link Account} of the container.
   */
  public Account parentAccount() {
    return parentAccount;
  }

  /**
   * Generates a {@link String} representation that uniquely identifies this container. The string
   * is in the format of {@code Container[accountId:accountName:containerId:containerName]}.
   * @return The {@link String} representation of this container.
   */
  @Override
  public String toString() {
    return "Container[" + parentAccount.id() + ":" + parentAccount.name() + ":" + id() + ":" + name() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Container container = (Container) o;

    if (id != container.id) {
      return false;
    }
    return parentAccount.equals(container.parentAccount);
  }

  @Override
  public int hashCode() {
    int result = (int) id;
    result = 31 * result + parentAccount.hashCode();
    return result;
  }
}
