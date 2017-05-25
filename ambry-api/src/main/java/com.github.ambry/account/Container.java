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

import org.json.JSONException;
import org.json.JSONObject;


/**
 * A representation of a container. A container virtually groups a number of blobs under the same {@link Account},
 * so that an operation on the container will be applied to all the blobs of the container. There can be multiple
 * containers under the the same {@link Account}, but containers cannot be nested. Eventually, one and only one
 * container needs to be specified when posting a blob. Container id is part of the blob’s properties, and cannot
 * be modified once the blob is created.
 *
 * Container name is provided by a user as an external reference to that container under an {@link Account}. Container
 * id is one-to-one mapped to container name, and serves as an internal identifier of a container. Container name/id
 * has to be distinct within the same {@link Account}, but can be the same across different {@link Account}s. Container
 * metadata is made in JSON, which is generic to contain additional information of the metadata.
 *
 * Version 1 of Container metadata in JSON is in the format below.
 *  {
 *    "containerName": "MyPrivateContainer",
 *    "description": "This is my private container",
 *    "isPrivate": "true",
 *    "containerId": 0,
 *    "version": 1,
 *    "status": "active"
 *    "parentAccountId": "101"
 *  }
 *
 *  A container object is immutable. To update a container of an account, refer to {@link ContainerBuilder} and
 *  {@link AccountBuilder}.
 */
public class Container {
  // static variables
  static final String CONTAINER_METADATA_VERSION_KEY = "version";
  static final String CONTAINER_NAME_KEY = "containerName";
  static final String CONTAINER_ID_KEY = "containerId";
  static final String CONTAINER_STATUS_KEY = "status";
  static final String CONTAINER_DESCRIPTION_KEY = "description";
  static final String CONTAINER_IS_PRIVATE_KEY = "isPrivate";
  static final String CONTAINER_PARENT_ACCOUNT_ID_KEY = "parentAccountId";
  static final short CONTAINER_METADATA_VERSION_1 = 1;
  // container field variables
  private final short id;
  private final String name;
  private final ContainerStatus status;
  private final String description;
  private final boolean isPrivate;
  private final short parentAccountId;
  private short version;

  /**
   * Constructor from container metadata.
   * @param metadata The metadata of the container in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  public Container(JSONObject metadata) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    this.version = (short) metadata.getInt(CONTAINER_METADATA_VERSION_KEY);
    switch (version) {
      case CONTAINER_METADATA_VERSION_1:
        this.id = (short) metadata.getInt(CONTAINER_ID_KEY);
        this.name = metadata.getString(CONTAINER_NAME_KEY);
        this.status = ContainerStatus.valueOf(metadata.getString(CONTAINER_STATUS_KEY));
        this.description = metadata.optString(CONTAINER_DESCRIPTION_KEY);
        this.isPrivate = metadata.getBoolean(CONTAINER_IS_PRIVATE_KEY);
        this.parentAccountId = (short) metadata.getInt(CONTAINER_PARENT_ACCOUNT_ID_KEY);
        break;

      default:
        throw new IllegalStateException("Unsupported container metadata version=" + version);
    }
  }

  /**
   * Gets the id of the container.
   * @return The id of the container.
   */
  public short getId() {
    return id;
  }

  /**
   * Gets the name of the container.
   * @return The name of the container.
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the status of the container.
   * @return The status of the container.
   */
  public ContainerStatus getStatus() {
    return status;
  }

  /**
   * Gets the description of the container.
   * @return The description of the container.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Gets the boolean value if the entries in this container is private.
   * @return A boolean indicating if the entries in this container are private.
   */
  public boolean getIsPrivate() {
    return isPrivate;
  }

  /**
   * Gets the if of the {@link Account} that owns this container.
   * @return The id of the parent {@link Account} of this container.
   */
  public short getParentAccountId() {
    return parentAccountId;
  }

  /**
   * Gets the metadata of the container.
   * @return The metadata of the container.
   * @throws JSONException If fails to compose metadata.
   */
  public JSONObject toJson() throws JSONException {
    JSONObject metadata;
    switch (version) {
      case CONTAINER_METADATA_VERSION_1:
        metadata = new JSONObject();
        metadata.put(CONTAINER_METADATA_VERSION_KEY, version);
        metadata.put(CONTAINER_ID_KEY, id);
        metadata.put(CONTAINER_NAME_KEY, name);
        metadata.put(CONTAINER_STATUS_KEY, status);
        metadata.put(CONTAINER_DESCRIPTION_KEY, description);
        metadata.put(CONTAINER_IS_PRIVATE_KEY, isPrivate);
        metadata.put(CONTAINER_PARENT_ACCOUNT_ID_KEY, parentAccountId);
        break;

      default:
        throw new IllegalStateException("Unsupported container metadata version=" + version);
    }
    return metadata;
  }

  /**
   * Generates a {@link String} representation that uniquely identifies this container. The string
   * is in the format of {@code Container[accountId:containerId]}.
   * @return The {@link String} representation of this container.
   */
  @Override
  public String toString() {
    return "Container[" + getParentAccountId() + ":" + getId() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Container)) {
      return false;
    }

    Container container = (Container) o;

    if (id != container.id) {
      return false;
    }
    if (isPrivate != container.isPrivate) {
      return false;
    }
    if (parentAccountId != container.parentAccountId) {
      return false;
    }
    if (!name.equals(container.name)) {
      return false;
    }
    return status == container.status;
  }

  @Override
  public int hashCode() {
    int result = (int) id;
    result = 31 * result + (int) parentAccountId;
    return result;
  }

  /**
   * Status of the container. {@code ACTIVE} means this container is in operational state, and {@code INACTIVE} means
   * the container has been deactivated.
   */
  public enum ContainerStatus {
    ACTIVE, INACTIVE
  }
}
