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
 *  }
 */
public class Container {
  // static variables
  public static final String CONTAINER_METADATA_VERSION_KEY = "version";
  public static final String CONTAINER_NAME_KEY = "containerName";
  public static final String CONTAINER_ID_KEY = "containerId";
  public static final String CONTAINER_STATUS_KEY = "status";
  public static final String CONTAINER_DESCRIPTION_KEY = "description";
  public static final String CONTAINER_IS_PRIVATE_KEY = "isPrivate";
  public static final String CONTAINER_STATUS_ACTIVE = "active";
  public static final String CONTAINER_STATUS_INACTIVE = "inactive";
  public static final short CONTAINER_METADATA_VERSION_1 = 1;
  // container field variables
  private final short id;
  private final String name;
  private final String status;
  private final String description;
  private final boolean isPrivate;
  private final Account parentAccount;
  private final JSONObject metadata;

  /**
   * Constructor from container metadata.
   * @param metadata The metadata of the container in JSON.
   * @param parentAccount The parent {@link Account} of the container.
   */
  public Container(JSONObject metadata, Account parentAccount) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    if (parentAccount == null) {
      throw new IllegalArgumentException("parentAccount cannot be null.");
    }
    short containerMetadataVersion = (short) metadata.getInt(CONTAINER_METADATA_VERSION_KEY);
    switch (containerMetadataVersion) {
      case CONTAINER_METADATA_VERSION_1:
        this.id = (short) metadata.getInt(CONTAINER_ID_KEY);
        this.name = metadata.getString(CONTAINER_NAME_KEY);
        this.status = metadata.getString(CONTAINER_STATUS_KEY);
        this.description = metadata.optString(CONTAINER_DESCRIPTION_KEY);
        this.isPrivate = metadata.getBoolean(CONTAINER_IS_PRIVATE_KEY);
        this.metadata = metadata;
        this.parentAccount = parentAccount;
        break;

      default:
        throw new IllegalArgumentException("Unsupported container metadata version " + containerMetadataVersion);
    }
  }

  /**
   * Constructor.
   * @param id The id of the container.
   * @param name The name of the container.
   * @param description The description of the container.
   * @param status The status of the container.
   * @param isPrivate {@code true} indicates that the entries in this container are private and can be accessed
   *                              only by the parent account; {@code false} otherwise.
   * @param parentAccount The parent {@link Account} of this Container.
   * @throws JSONException
   */
  public Container(short id, String name, String status, String description, boolean isPrivate, Account parentAccount)
      throws JSONException {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null.");
    }
    if (parentAccount == null) {
      throw new IllegalArgumentException("parentAccount cannot be null.");
    }
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.isPrivate = isPrivate;
    this.parentAccount = parentAccount;
    metadata = new JSONObject();
    metadata.put(CONTAINER_METADATA_VERSION_KEY, CONTAINER_METADATA_VERSION_1);
    metadata.put(CONTAINER_ID_KEY, id);
    metadata.put(CONTAINER_NAME_KEY, name);
    metadata.put(CONTAINER_STATUS_KEY, status);
    metadata.put(CONTAINER_DESCRIPTION_KEY, description);
    metadata.put(CONTAINER_IS_PRIVATE_KEY, isPrivate);
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
  public String getStatus() {
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
   * Gets the boolean value if the entries in this container are private.
   * @return A boolean indicating if the entries in this container are private.
   */
  public boolean getPrivate() {
    return isPrivate;
  }

  /**
   * Gets the {@link Account} that owns this container.
   * @return The parent {@link Account} of this container.
   */
  public Account getParentAccount() {
    return parentAccount;
  }

  /**
   * Gets the metadata of the container.
   * @return The metadata of the container.
   */
  public JSONObject getMetadata() {
    return metadata;
  }

  /**
   * Generates a {@link String} representation that uniquely identifies this container. The string
   * is in the format of {@code Container[accountId:accountName:containerId:containerName]}.
   * @return The {@link String} representation of this container.
   */
  @Override
  public String toString() {
    return "Container[" + parentAccount.getId() + ":" + parentAccount.getName() + ":" + getId() + ":" + getName() + "]";
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
    if (!name.equals(container.name)) {
      return false;
    }
    if (status != null ? !status.equals(container.status) : container.status != null) {
      return false;
    }
    if (description != null ? !description.equals(container.description) : container.description != null) {
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
