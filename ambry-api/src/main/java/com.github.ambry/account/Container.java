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
   * The id for the containers to be associated with the blobs that are put without specifying a target container,
   * but are specified public. {@link #DEFAULT_PUBLIC_CONTAINER} is one of the containers that use it.
   */
  public static final short DEFAULT_PUBLIC_CONTAINER_ID = 0;

  /**
   * The id for the containers to be associated with the blobs that are put without specifying a target container,
   * but are specified private. {@link #DEFAULT_PRIVATE_CONTAINER} is one of the containers that use it.
   */
  public static final short DEFAULT_PRIVATE_CONTAINER_ID = 1;

  /**
   * The name of {@link #UNKNOWN_CONTAINER}.
   */
  public static final String UNKNOWN_CONTAINER_NAME = "ambry-unknown-container";

  /**
   * The name for the containers to be associated with the blobs that are put without specifying a target container,
   * but are specified public. {@link #DEFAULT_PUBLIC_CONTAINER} is one of the containers that use it.
   */
  public static final String DEFAULT_PUBLIC_CONTAINER_NAME = "default-public-container";

  /**
   * The name for the containers to be associated with the blobs that are put without specifying a target container,
   * but are specified private. {@link #DEFAULT_PRIVATE_CONTAINER} is one of the containers that use it.
   */
  public static final String DEFAULT_PRIVATE_CONTAINER_NAME = "default-private-container";

  /**
   * The status of {@link #UNKNOWN_CONTAINER}.
   */
  public static final ContainerStatus UNKNOWN_CONTAINER_STATUS = ContainerStatus.ACTIVE;

  /**
   * The status for the containers to be associated with the blobs that are put without specifying a target container,
   * but are specified public. {@link #DEFAULT_PUBLIC_CONTAINER} is one of the containers that use it.
   */
  public static final ContainerStatus DEFAULT_PUBLIC_CONTAINER_STATUS = ContainerStatus.ACTIVE;

  /**
   * The status for the containers to be associated with the blobs that are put without specifying a target container,
   * but are specified private. {@link #DEFAULT_PRIVATE_CONTAINER} is one of the containers that use it.
   */
  public static final ContainerStatus DEFAULT_PRIVATE_CONTAINER_STATUS = ContainerStatus.ACTIVE;

  /**
   * The description of {@link #UNKNOWN_CONTAINER}.
   */
  public static final String UNKNOWN_CONTAINER_DESCRIPTION =
      "This is a container for the blobs without specifying a target account and container when they are put";

  /**
   * The description for the containers to be associated with the blobs that are put without specifying a target
   * container, but are specified public. {@link #DEFAULT_PUBLIC_CONTAINER} is one of the containers that use it.
   */
  public static final String DEFAULT_PUBLIC_CONTAINER_DESCRIPTION =
      "This is a container for the blobs without specifying a target account and container when they are put and isPrivate flag is false";

  /**
   * The description for the containers to be associated with the blobs that are put without specifying a target
   * container, but are specified private. {@link #DEFAULT_PRIVATE_CONTAINER} is one of the containers that use it.
   */
  public static final String DEFAULT_PRIVATE_CONTAINER_DESCRIPTION =
      "This is a container for the blobs without specifying a target account and container when they are put and isPrivate flag is true";

  /**
   * The encryption setting of {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_ENCRYPTED_SETTING = false;

  /**
   * The encryption setting of {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_ENCRYPTED_SETTING = false;

  /**
   * The encryption setting of {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_ENCRYPTED_SETTING = false;

  /**
   * The previously encrypted flag for {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING = false;

  /**
   * The previously encrypted flag for {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING = false;

  /**
   * The previously encrypted flag for {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING = false;

  /**
   * The cache setting of {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_CACHEABLE_SETTING = true;

  /**
   * The cache setting of {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_CACHEABLE_SETTING = true;

  /**
   * The cache setting of {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_CACHEABLE_SETTING = false;

  /**
   * The media scan disabled setting for {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_MEDIA_SCAN_DISABLED_SETTING = false;

  /**
   * The media scan disabled setting for {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_MEDIA_SCAN_DISABLED_SETTING = false;

  /**
   * The media scan disabled setting for {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_MEDIA_SCAN_DISABLED_SETTING = false;

  /**
   * The parent account id of {@link #UNKNOWN_CONTAINER}.
   */
  public static final short UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID = Account.UNKNOWN_ACCOUNT_ID;

  /**
   * The parent account id of {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final short DEFAULT_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID = Account.UNKNOWN_ACCOUNT_ID;

  /**
   * The parent account id of {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final short DEFAULT_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID = Account.UNKNOWN_ACCOUNT_ID;

  /**
   * A container defined specifically for the blobs put without specifying target account and container. In the
   * pre-containerization world, a put-blob request does not carry any information which account/container to store
   * the blob. These blobs are literally put into this container, because the target container information is unknown.
   */
  public static final Container UNKNOWN_CONTAINER =
      new Container(UNKNOWN_CONTAINER_ID, UNKNOWN_CONTAINER_NAME, UNKNOWN_CONTAINER_STATUS,
          UNKNOWN_CONTAINER_DESCRIPTION, UNKNOWN_CONTAINER_ENCRYPTED_SETTING,
          UNKNOWN_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, UNKNOWN_CONTAINER_CACHEABLE_SETTING,
          UNKNOWN_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID);

  /**
   * A container defined specifically for the blobs put without specifying target container but isPrivate flag is
   * set to {@code false}.
   */
  public static final Container DEFAULT_PUBLIC_CONTAINER =
      new Container(DEFAULT_PUBLIC_CONTAINER_ID, DEFAULT_PUBLIC_CONTAINER_NAME, DEFAULT_PUBLIC_CONTAINER_STATUS,
          DEFAULT_PUBLIC_CONTAINER_DESCRIPTION, DEFAULT_PUBLIC_CONTAINER_ENCRYPTED_SETTING,
          DEFAULT_PUBLIC_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, DEFAULT_PUBLIC_CONTAINER_CACHEABLE_SETTING,
          DEFAULT_PUBLIC_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, DEFAULT_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID);

  /**
   * A container defined specifically for the blobs put without specifying target container but isPrivate flag is
   * set to {@code true}.
   */
  public static final Container DEFAULT_PRIVATE_CONTAINER =
      new Container(DEFAULT_PRIVATE_CONTAINER_ID, DEFAULT_PRIVATE_CONTAINER_NAME, DEFAULT_PRIVATE_CONTAINER_STATUS,
          DEFAULT_PRIVATE_CONTAINER_DESCRIPTION, DEFAULT_PRIVATE_CONTAINER_ENCRYPTED_SETTING,
          DEFAULT_PRIVATE_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, DEFAULT_PRIVATE_CONTAINER_CACHEABLE_SETTING,
          DEFAULT_PRIVATE_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, DEFAULT_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID);

  // static variables
  static final String JSON_VERSION_KEY = "version";
  static final String CONTAINER_NAME_KEY = "containerName";
  static final String CONTAINER_ID_KEY = "containerId";
  static final String STATUS_KEY = "status";
  static final String DESCRIPTION_KEY = "description";
  static final String IS_PRIVATE_KEY = "isPrivate";
  static final String ENCRYPTED_KEY = "encrypted";
  static final String PREVIOUSLY_ENCRYPTED_KEY = "previouslyEncrypted";
  static final String CACHEABLE_KEY = "cacheable";
  static final String MEDIA_SCAN_DISABLED = "mediaScanDisabled";
  static final String PARENT_ACCOUNT_ID_KEY = "parentAccountId";
  static final short JSON_VERSION_1 = 1;
  static final short JSON_VERSION_2 = 2;
  static final short CURRENT_JSON_VERSION = JSON_VERSION_1;

  // field default values
  static final boolean ENCRYPTED_DEFAULT_VALUE = false;
  static final boolean PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE = ENCRYPTED_DEFAULT_VALUE;
  static final boolean MEDIA_SCAN_DISABLED_DEFAULT_VALUE = false;
  static final boolean CACHEABLE_DEFAULT_VALUE = true;

  // container field variables
  private final short id;
  private final String name;
  private final ContainerStatus status;
  private final String description;
  private final boolean encrypted;
  private final boolean previouslyEncrypted;
  private final boolean cacheable;
  private final boolean mediaScanDisabled;
  private final short parentAccountId;

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
        id = (short) metadata.getInt(CONTAINER_ID_KEY);
        name = metadata.getString(CONTAINER_NAME_KEY);
        status = ContainerStatus.valueOf(metadata.getString(STATUS_KEY));
        description = metadata.optString(DESCRIPTION_KEY);
        parentAccountId = (short) metadata.getInt(PARENT_ACCOUNT_ID_KEY);
        encrypted = ENCRYPTED_DEFAULT_VALUE;
        previouslyEncrypted = PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;
        cacheable = !metadata.getBoolean(IS_PRIVATE_KEY);
        mediaScanDisabled = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;
        break;
      case JSON_VERSION_2:
        id = (short) metadata.getInt(CONTAINER_ID_KEY);
        name = metadata.getString(CONTAINER_NAME_KEY);
        status = ContainerStatus.valueOf(metadata.getString(STATUS_KEY));
        description = metadata.optString(DESCRIPTION_KEY);
        parentAccountId = (short) metadata.getInt(PARENT_ACCOUNT_ID_KEY);
        encrypted = metadata.optBoolean(ENCRYPTED_KEY, ENCRYPTED_DEFAULT_VALUE);
        previouslyEncrypted = metadata.optBoolean(PREVIOUSLY_ENCRYPTED_KEY, PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE);
        cacheable = metadata.optBoolean(CACHEABLE_KEY, CACHEABLE_DEFAULT_VALUE);
        mediaScanDisabled = metadata.optBoolean(MEDIA_SCAN_DISABLED, MEDIA_SCAN_DISABLED_DEFAULT_VALUE);
        break;
      default:
        throw new IllegalStateException("Unsupported container json version=" + metadataVersion);
    }
    checkPreconditions();
  }

  /**
   * Constructor that takes individual arguments. Cannot be null.
   * @param id The id of the container.
   * @param name The name of the container. Cannot be null.
   * @param status The status of the container. Cannot be null.
   * @param description The description of the container. Can be null.
   * @param encrypted {@code true} if blobs in the {@link Container} should be encrypted, {@code false} otherwise.
   * @param previouslyEncrypted {@code true} if this {@link Container} was encrypted in the past, or currently, and a
   *                            subset of blobs in it could still be encrypted.
   * @param cacheable {@code true} if cache control headers should be set to allow CDNs and browsers to cache blobs in
   *                  this container.
   * @param mediaScanDisabled {@code true} if media scanning for content in this container should be disabled.
   * @param parentAccountId The id of the parent {@link Account} of this container.
   */
  Container(short id, String name, ContainerStatus status, String description, boolean encrypted,
      boolean previouslyEncrypted, boolean cacheable, boolean mediaScanDisabled, short parentAccountId) {
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.encrypted = encrypted;
    this.previouslyEncrypted = previouslyEncrypted;
    this.cacheable = cacheable;
    this.mediaScanDisabled = mediaScanDisabled;
    this.parentAccountId = parentAccountId;
    checkPreconditions();
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
   * Gets the metadata of the container.
   * @return The metadata of the container.
   * @throws JSONException If fails to compose metadata.
   */
  public JSONObject toJson() throws JSONException {
    JSONObject metadata = new JSONObject();
    metadata.put(JSON_VERSION_KEY, CURRENT_JSON_VERSION);
    // writing in V1
    metadata.put(CONTAINER_ID_KEY, id);
    metadata.put(CONTAINER_NAME_KEY, name);
    metadata.put(STATUS_KEY, status);
    metadata.put(DESCRIPTION_KEY, description);
    metadata.put(IS_PRIVATE_KEY, !cacheable);
    metadata.put(PARENT_ACCOUNT_ID_KEY, parentAccountId);
    return metadata;
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
   * @return {@code true} if blobs in the {@link Container} should be encrypted, {@code false} otherwise.
   */
  public boolean isEncrypted() {
    return encrypted;
  }

  /**
   * @return {@code true} if this {@link Container} was encrypted in the past, and a subset of blobs in it could still
   *         be encrypted.
   */
  public boolean wasPreviouslyEncrypted() {
    return previouslyEncrypted;
  }

  /**
   * @return {@code true} if cache control headers should be set to allow CDNs and browsers to cache blobs in this
   * container.
   */
  public boolean isCacheable() {
    return cacheable;
  }

  /**
   * @return {@code true} if media scans should be disabled on content created in this container.
   */
  public boolean isMediaScanDisabled() {
    return mediaScanDisabled;
  }

  /**
   * Gets the if of the {@link Account} that owns this container.
   * @return The id of the parent {@link Account} of this container.
   */
  public short getParentAccountId() {
    return parentAccountId;
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
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Container container = (Container) o;

    if (id != container.id) {
      return false;
    }
    if (encrypted != container.encrypted) {
      return false;
    }
    if (previouslyEncrypted != container.previouslyEncrypted) {
      return false;
    }
    if (cacheable != container.cacheable) {
      return false;
    }
    if (mediaScanDisabled != container.mediaScanDisabled) {
      return false;
    }
    if (parentAccountId != container.parentAccountId) {
      return false;
    }
    if (!name.equals(container.name)) {
      return false;
    }
    if (status != container.status) {
      return false;
    }
    return description != null ? description.equals(container.description) : container.description == null;
  }

  @Override
  public int hashCode() {
    int result = (int) id;
    result = 31 * result + (int) parentAccountId;
    return result;
  }

  /**
   * Checks if any required fields is missing for a {@link Container} or for any incompatible settings.
   */
  private void checkPreconditions() {
    if (name == null || status == null) {
      throw new IllegalStateException("Either of required fields name=" + name + " or status=" + status + " is null");
    }
    if (encrypted && !previouslyEncrypted) {
      throw new IllegalStateException("previouslyEncrypted should be true if the container is currently encrypted");
    }
  }

  /**
   * Status of the container. {@code ACTIVE} means this container is in operational state, and {@code INACTIVE} means
   * the container has been deactivated.
   */
  public enum ContainerStatus {
    ACTIVE, INACTIVE
  }
}
