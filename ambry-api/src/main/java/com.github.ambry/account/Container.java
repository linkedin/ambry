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
 * <p>
 *   A representation of a container. A container virtually groups a number of blobs under the same {@link Account},
 * so that an operation on a container can be applied to all the blobs of the container. When posting a blob, a
 * container needs to be specified. There can be multiple containers under the the same {@link Account}, but containers
 * cannot be nested.
 * </p>
 * <p>
 *   Container name is provided by a user as an external reference to that container. Container id is an internal
 *   identifier of the container, and is one-to-one mapped to a container name. Container name/id has to be distinct
 *   within the same {@link Account}, but can be the same across different {@link Account}s.
 * </p>
 * <p>
 *   Container is serialized into {@link JSONObject} in the {@code CURRENT_JSON_VERSION}, which is version 1
 *   for now. Below lists all the metadata versions and their formats:
 * </p>
 *  <pre><code>
 *  {
 *    "containerName": "MyPrivateContainer",
 *    "description": "This is my private container",
 *    "isPrivate": "true",
 *    "containerId": 0,
 *    "version": 1,
 *    "status": "active"
 *    "parentAccountId": "101"
 *  }
 *  </code></pre>
 *  <p>
 *    A container object is immutable. To update a container, refer to {@link ContainerBuilder} and
 *  {@link AccountBuilder}.
 *  </p>
 */
public class Container {
  /**
   * The id of {@link #UNKNOWN_CONTAINER}.
   */
  public static final short UNKNOWN_CONTAINER_ID = -1;

  /**
   * The id of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final short UNKNOWN_PUBLIC_CONTAINER_ID = -2;

  /**
   * The id of {@link #UNKNOWN_PRIVATE_CONTAINER}.
   */
  public static final short UNKNOWN_PRIVATE_CONTAINER_ID = -3;

  /**
   * The name of {@link #UNKNOWN_CONTAINER}.
   */
  public static final String UNKNOWN_CONTAINER_NAME = "ambry-unknown-container-name";

  /**
   * The name of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final String UNKNOWN_PUBLIC_CONTAINER_NAME = "ambry-unknown-public-container-name";

  /**
   * The name of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final String UNKNOWN_PRIVATE_CONTAINER_NAME = "ambry-unknown-private-container-name";

  /**
   * The status of {@link #UNKNOWN_CONTAINER}.
   */
  public static final ContainerStatus UNKNOWN_CONTAINER_STATUS = ContainerStatus.ACTIVE;

  /**
   * The status of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final ContainerStatus UNKNOWN_PUBLIC_CONTAINER_STATUS = ContainerStatus.ACTIVE;

  /**
   * The status of {@link #UNKNOWN_PRIVATE_CONTAINER}.
   */
  public static final ContainerStatus UNKNOWN_PRIVATE_CONTAINER_STATUS = ContainerStatus.ACTIVE;

  /**
   * The description of {@link #UNKNOWN_CONTAINER}.
   */
  public static final String UNKNOWN_CONTAINER_DESCRIPTION =
      "This is a container for the blobs without specifying a target account and container when they are put";

  /**
   * The description of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final String UNKNOWN_PUBLIC_CONTAINER_DESCRIPTION =
      "This is a container for the blobs without specifying a target account and container when they are put and isPrivate flag is false";

  /**
   * The description of {@link #UNKNOWN_PRIVATE_CONTAINER}.
   */
  public static final String UNKNOWN_PRIVATE_CONTAINER_DESCRIPTION =
      "This is a container for the blobs without specifying a target account and container when they are put and isPrivate flag is true";

  /**
   * The privacy setting of {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_IS_PRIVATE_SETTING = false;

  /**
   * The privacy setting of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final boolean UNKNOWN_PUBLIC_CONTAINER_IS_PRIVATE_SETTING = false;

  /**
   * The privacy setting of {@link #UNKNOWN_PRIVATE_CONTAINER}.
   */
  public static final boolean UNKNOWN_PRIVATE_CONTAINER_IS_PRIVATE_SETTING = true;

  /**
   * The parent account id of {@link #UNKNOWN_CONTAINER}.
   */
  public static final short UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID = Account.UNKNOWN_ACCOUNT_ID;

  /**
   * The parent account id of {@link #UNKNOWN_PUBLIC_CONTAINER}.
   */
  public static final short UNKNOWN_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID = Account.UNKNOWN_ACCOUNT_ID;

  /**
   * The parent account id of {@link #UNKNOWN_PRIVATE_CONTAINER}.
   */
  public static final short UNKNOWN_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID = Account.UNKNOWN_ACCOUNT_ID;

  /**
   * A container defined specifically for the blobs put without specifying target account and container. In the
   * pre-containerization world, a put-blob request does not carry any information which account/container to store
   * the blob. These blobs are literally put into this container, because the target container information is unknown.
   */
  public static final Container UNKNOWN_CONTAINER =
      new Container(UNKNOWN_CONTAINER_ID, UNKNOWN_CONTAINER_NAME, UNKNOWN_CONTAINER_STATUS,
          UNKNOWN_CONTAINER_DESCRIPTION, UNKNOWN_CONTAINER_IS_PRIVATE_SETTING, UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID);

  /**
   * A container defined specifically for the blobs put without specifying target container but isPrivate flag is
   * set to {@code false}.
   */
  public static final Container UNKNOWN_PUBLIC_CONTAINER =
      new Container(UNKNOWN_PUBLIC_CONTAINER_ID, UNKNOWN_PUBLIC_CONTAINER_NAME, UNKNOWN_PUBLIC_CONTAINER_STATUS,
          UNKNOWN_PUBLIC_CONTAINER_DESCRIPTION, UNKNOWN_PUBLIC_CONTAINER_IS_PRIVATE_SETTING,
          UNKNOWN_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID);

  /**
   * A container defined specifically for the blobs put without specifying target container but isPrivate flag is
   * set to {@code true}.
   */
  public static final Container UNKNOWN_PRIVATE_CONTAINER =
      new Container(UNKNOWN_PRIVATE_CONTAINER_ID, UNKNOWN_PRIVATE_CONTAINER_NAME, UNKNOWN_PRIVATE_CONTAINER_STATUS,
          UNKNOWN_PRIVATE_CONTAINER_DESCRIPTION, UNKNOWN_PRIVATE_CONTAINER_IS_PRIVATE_SETTING,
          UNKNOWN_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID);

  // static variables
  static final String JSON_VERSION_KEY = "version";
  static final String CONTAINER_NAME_KEY = "containerName";
  static final String CONTAINER_ID_KEY = "containerId";
  static final String STATUS_KEY = "status";
  static final String DESCRIPTION_KEY = "description";
  static final String IS_PRIVATE_KEY = "isPrivate";
  static final String PARENT_ACCOUNT_ID_KEY = "parentAccountId";
  static final short JSON_VERSION_1 = 1;
  static final short CURRENT_JSON_VERSION = JSON_VERSION_1;
  // container field variables
  private final Short id;
  private final String name;
  private final ContainerStatus status;
  private final String description;
  private final Boolean isPrivate;
  private final Short parentAccountId;

  /**
   * Constructing an {@link Container} object from container metadata.
   * @param metadata The metadata of the container in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  private Container(JSONObject metadata) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    short metadataVersion = (short) metadata.getInt(JSON_VERSION_KEY);
    switch (metadataVersion) {
      case JSON_VERSION_1:
        this.id = (short) metadata.getInt(CONTAINER_ID_KEY);
        this.name = metadata.getString(CONTAINER_NAME_KEY);
        this.status = ContainerStatus.valueOf(metadata.getString(STATUS_KEY));
        this.description = metadata.optString(DESCRIPTION_KEY);
        this.isPrivate = metadata.getBoolean(IS_PRIVATE_KEY);
        this.parentAccountId = (short) metadata.getInt(PARENT_ACCOUNT_ID_KEY);
        checkRequiredFields();
        break;

      default:
        throw new IllegalStateException("Unsupported container json version=" + metadataVersion);
    }
  }

  /**
   * Constructor that takes individual arguments. Cannot be null.
   * @param id The id of the container. Cannot be null.
   * @param name The name of the container. Cannot be null.
   * @param status The status of the container. Cannot be null.
   * @param description The description of the container. Can be null.
   * @param isPrivate The privacy setting of the container. Cannot be null.
   * @param parentAccountId The id of the parent {@link Account} of this container. Cannot be null.
   */
  Container(Short id, String name, ContainerStatus status, String description, Boolean isPrivate,
      Short parentAccountId) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.isPrivate = isPrivate;
    this.parentAccountId = parentAccountId;
    checkRequiredFields();
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
  public boolean isPrivate() {
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
    JSONObject metadata = new JSONObject();
    metadata.put(JSON_VERSION_KEY, CURRENT_JSON_VERSION);
    metadata.put(CONTAINER_ID_KEY, id);
    metadata.put(CONTAINER_NAME_KEY, name);
    metadata.put(STATUS_KEY, status);
    metadata.put(DESCRIPTION_KEY, description);
    metadata.put(IS_PRIVATE_KEY, isPrivate);
    metadata.put(PARENT_ACCOUNT_ID_KEY, parentAccountId);
    return metadata;
  }

  /**
   * Generates a String representation that uniquely identifies this container. The string
   * is in the format of {@code Container[accountId:containerId]}.
   * @return The String representation of this container.
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

    if (!id.equals(container.id)) {
      return false;
    }
    if (!name.equals(container.name)) {
      return false;
    }
    if (status != container.status) {
      return false;
    }
    if (!isPrivate.equals(container.isPrivate)) {
      return false;
    }
    return parentAccountId.equals(container.parentAccountId);
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

  /**
   * Deserializes a {@link JSONObject} to a container object.
   * @param json The {@link JSONObject} to deserialize.
   * @return A container object deserialized from the {@link JSONObject}.
   * @throws JSONException If parsing the {@link JSONObject} fails.
   */
  public static Container fromJson(JSONObject json) throws JSONException {
    return new Container(json);
  }

  /**
   * Checks if any required fields is missing for a {@link Container}.
   */
  private void checkRequiredFields() {
    if (id == null || name == null || status == null || isPrivate == null || parentAccountId == null) {
      throw new IllegalStateException(
          "Either of required fields id=" + id + " or name=" + name + " or status=" + status + " or isPrivate="
              + isPrivate + " or parentAccountId=" + parentAccountId + " is null");
    }
  }
}
