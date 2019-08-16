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

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.json.JSONArray;
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
 *   Container is serialized into {@link JSONObject} in the {@code currentJsonVersion}, which is version 1
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

  // constants
  static final String JSON_VERSION_KEY = "version";
  static final String CONTAINER_NAME_KEY = "containerName";
  static final String CONTAINER_ID_KEY = "containerId";
  static final String STATUS_KEY = "status";
  static final String DESCRIPTION_KEY = "description";
  static final String IS_PRIVATE_KEY = "isPrivate";
  static final String BACKUP_ENABLED_KEY = "backupEnabled";
  static final String ENCRYPTED_KEY = "encrypted";
  static final String PREVIOUSLY_ENCRYPTED_KEY = "previouslyEncrypted";
  static final String CACHEABLE_KEY = "cacheable";
  static final String MEDIA_SCAN_DISABLED_KEY = "mediaScanDisabled";
  static final String REPLICATION_POLICY_KEY = "replicationPolicy";
  static final String TTL_REQUIRED_KEY = "ttlRequired";
  static final String SECURE_PATH_REQUIRED_KEY = "securePathRequired";
  static final String CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD = "contentTypeWhitelistForFilenamesOnDownload";
  static final String PARENT_ACCOUNT_ID_KEY = "parentAccountId";
  static final boolean BACKUP_ENABLED_DEFAULT_VALUE = false;
  static final boolean ENCRYPTED_DEFAULT_VALUE = false;
  static final boolean PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE = ENCRYPTED_DEFAULT_VALUE;
  static final boolean MEDIA_SCAN_DISABLED_DEFAULT_VALUE = false;
  static final boolean TTL_REQUIRED_DEFAULT_VALUE = true;
  static final boolean SECURE_PATH_REQUIRED_DEFAULT_VALUE = false;
  static final boolean CACHEABLE_DEFAULT_VALUE = true;
  static final Set<String> CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE = Collections.emptySet();

  public static final short JSON_VERSION_1 = 1;
  public static final short JSON_VERSION_2 = 2;

  /**
   * The current version to serialize in. This can be set through the {@link #setCurrentJsonVersion(short)} method.
   * This variable should be declared/initialized before the default containers are constructed. Otherwise, this
   * field will not be initialized when those constructors are called and it will fail.
   */
  private static short currentJsonVersion = JSON_VERSION_2;

  /**
   * The id of {@link #UNKNOWN_CONTAINER}.
   */
  public static final short UNKNOWN_CONTAINER_ID = -1;

  /**
   * The id for HelixAccountService to store {@link Account} metadata.
   */
  public static final short HELIX_ACCOUNT_SERVICE_CONTAINER_ID = -2;

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
  public static final boolean UNKNOWN_CONTAINER_ENCRYPTED_SETTING = ENCRYPTED_DEFAULT_VALUE;

  /**
   * The encryption setting of {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_ENCRYPTED_SETTING = ENCRYPTED_DEFAULT_VALUE;

  /**
   * The encryption setting of {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_ENCRYPTED_SETTING = ENCRYPTED_DEFAULT_VALUE;

  /**
   * The previously encrypted flag for {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING = PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;

  /**
   * The previously encrypted flag for {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING =
      PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;

  /**
   * The previously encrypted flag for {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING =
      PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;

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
  public static final boolean UNKNOWN_CONTAINER_MEDIA_SCAN_DISABLED_SETTING = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;

  /**
   * The media scan disabled setting for {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_MEDIA_SCAN_DISABLED_SETTING = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;

  /**
   * The media scan disabled setting for {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_MEDIA_SCAN_DISABLED_SETTING = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;

  /**
   * The ttl required setting for {@link #UNKNOWN_CONTAINER}.
   */
  public static final boolean UNKNOWN_CONTAINER_TTL_REQUIRED_SETTING = TTL_REQUIRED_DEFAULT_VALUE;

  /**
   * The ttl required setting for {@link #DEFAULT_PUBLIC_CONTAINER}.
   */
  public static final boolean DEFAULT_PUBLIC_CONTAINER_TTL_REQUIRED_SETTING = TTL_REQUIRED_DEFAULT_VALUE;

  /**
   * The ttl required setting for {@link #DEFAULT_PRIVATE_CONTAINER}.
   */
  public static final boolean DEFAULT_PRIVATE_CONTAINER_TTL_REQUIRED_SETTING = TTL_REQUIRED_DEFAULT_VALUE;

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
   *
   * DO NOT USE IN PRODUCTION CODE.
   */
  @Deprecated
  public static final Container UNKNOWN_CONTAINER =
      new Container(UNKNOWN_CONTAINER_ID, UNKNOWN_CONTAINER_NAME, UNKNOWN_CONTAINER_STATUS,
          UNKNOWN_CONTAINER_DESCRIPTION, UNKNOWN_CONTAINER_ENCRYPTED_SETTING,
          UNKNOWN_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, UNKNOWN_CONTAINER_CACHEABLE_SETTING,
          UNKNOWN_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, null, UNKNOWN_CONTAINER_TTL_REQUIRED_SETTING,
          SECURE_PATH_REQUIRED_DEFAULT_VALUE, CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE,
          BACKUP_ENABLED_DEFAULT_VALUE, UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID);

  /**
   * A container defined specifically for the blobs put without specifying target container but isPrivate flag is
   * set to {@code false}.
   *
   * DO NOT USE IN PRODUCTION CODE.
   */
  @Deprecated
  public static final Container DEFAULT_PUBLIC_CONTAINER =
      new Container(DEFAULT_PUBLIC_CONTAINER_ID, DEFAULT_PUBLIC_CONTAINER_NAME, DEFAULT_PUBLIC_CONTAINER_STATUS,
          DEFAULT_PUBLIC_CONTAINER_DESCRIPTION, DEFAULT_PUBLIC_CONTAINER_ENCRYPTED_SETTING,
          DEFAULT_PUBLIC_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, DEFAULT_PUBLIC_CONTAINER_CACHEABLE_SETTING,
          DEFAULT_PUBLIC_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, null, DEFAULT_PUBLIC_CONTAINER_TTL_REQUIRED_SETTING,
          SECURE_PATH_REQUIRED_DEFAULT_VALUE, CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE,
          BACKUP_ENABLED_DEFAULT_VALUE, DEFAULT_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID);

  /**
   * A container defined specifically for the blobs put without specifying target container but isPrivate flag is
   * set to {@code true}.
   *
   * DO NOT USE IN PRODUCTION CODE.
   */
  @Deprecated
  public static final Container DEFAULT_PRIVATE_CONTAINER =
      new Container(DEFAULT_PRIVATE_CONTAINER_ID, DEFAULT_PRIVATE_CONTAINER_NAME, DEFAULT_PRIVATE_CONTAINER_STATUS,
          DEFAULT_PRIVATE_CONTAINER_DESCRIPTION, DEFAULT_PRIVATE_CONTAINER_ENCRYPTED_SETTING,
          DEFAULT_PRIVATE_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, DEFAULT_PRIVATE_CONTAINER_CACHEABLE_SETTING,
          DEFAULT_PRIVATE_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, null, DEFAULT_PRIVATE_CONTAINER_TTL_REQUIRED_SETTING,
          SECURE_PATH_REQUIRED_DEFAULT_VALUE, CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE,
          BACKUP_ENABLED_DEFAULT_VALUE, DEFAULT_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID);

  // container field variables
  private final short id;
  private final String name;
  private final ContainerStatus status;
  private final String description;
  private final boolean encrypted;
  private final boolean previouslyEncrypted;
  private final boolean cacheable;
  private final boolean backupEnabled;
  private final boolean mediaScanDisabled;
  private final String replicationPolicy;
  private final boolean ttlRequired;
  private final boolean securePathRequired;
  private final Set<String> contentTypeWhitelistForFilenamesOnDownload;
  private final short parentAccountId;

  /**
   * Constructing an {@link Container} object from container metadata.
   * @param metadata The metadata of the container in JSON.
   * @throws JSONException If fails to parse metadata.
   */
  private Container(JSONObject metadata, short parentAccountId) throws JSONException {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata cannot be null.");
    }
    this.parentAccountId = parentAccountId;
    short metadataVersion = (short) metadata.getInt(JSON_VERSION_KEY);
    switch (metadataVersion) {
      case JSON_VERSION_1:
        id = (short) metadata.getInt(CONTAINER_ID_KEY);
        name = metadata.getString(CONTAINER_NAME_KEY);
        status = ContainerStatus.valueOf(metadata.getString(STATUS_KEY));
        description = metadata.optString(DESCRIPTION_KEY);
        encrypted = ENCRYPTED_DEFAULT_VALUE;
        previouslyEncrypted = PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;
        cacheable = !metadata.getBoolean(IS_PRIVATE_KEY);
        backupEnabled = BACKUP_ENABLED_DEFAULT_VALUE;
        mediaScanDisabled = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;
        replicationPolicy = null;
        ttlRequired = TTL_REQUIRED_DEFAULT_VALUE;
        securePathRequired = SECURE_PATH_REQUIRED_DEFAULT_VALUE;
        contentTypeWhitelistForFilenamesOnDownload = CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE;
        break;
      case JSON_VERSION_2:
        id = (short) metadata.getInt(CONTAINER_ID_KEY);
        name = metadata.getString(CONTAINER_NAME_KEY);
        status = ContainerStatus.valueOf(metadata.getString(STATUS_KEY));
        description = metadata.optString(DESCRIPTION_KEY);
        encrypted = metadata.optBoolean(ENCRYPTED_KEY, ENCRYPTED_DEFAULT_VALUE);
        previouslyEncrypted = metadata.optBoolean(PREVIOUSLY_ENCRYPTED_KEY, PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE);
        cacheable = metadata.optBoolean(CACHEABLE_KEY, CACHEABLE_DEFAULT_VALUE);
        backupEnabled = metadata.optBoolean(BACKUP_ENABLED_KEY, BACKUP_ENABLED_DEFAULT_VALUE);
        mediaScanDisabled = metadata.optBoolean(MEDIA_SCAN_DISABLED_KEY, MEDIA_SCAN_DISABLED_DEFAULT_VALUE);
        replicationPolicy = metadata.optString(REPLICATION_POLICY_KEY, null);
        ttlRequired = metadata.optBoolean(TTL_REQUIRED_KEY, TTL_REQUIRED_DEFAULT_VALUE);
        securePathRequired = metadata.optBoolean(SECURE_PATH_REQUIRED_KEY, SECURE_PATH_REQUIRED_DEFAULT_VALUE);
        JSONArray contentTypeWhitelistForFilenamesOnDownloadJson =
            metadata.optJSONArray(CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD);
        if (contentTypeWhitelistForFilenamesOnDownloadJson != null) {
          contentTypeWhitelistForFilenamesOnDownload = new HashSet<>();
          contentTypeWhitelistForFilenamesOnDownloadJson.forEach(
              contentType -> contentTypeWhitelistForFilenamesOnDownload.add(contentType.toString()));
        } else {
          contentTypeWhitelistForFilenamesOnDownload = CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE;
        }
        break;
      default:
        throw new IllegalStateException("Unsupported container json version=" + metadataVersion);
    }
    checkPreconditions(name, status, encrypted, previouslyEncrypted);
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
   * @param replicationPolicy the replication policy to use. If {@code null}, the cluster's default will be used.
   * @param ttlRequired {@code true} if ttl is required on content created in this container.
   * @param securePathRequired {@code true} if secure path validation is required in this container.
   * @param contentTypeWhitelistForFilenamesOnDownload the set of content types for which the filename can be sent on
   *                                                   download
   * @param backupEnabled Whether backup is enabled for this container or not
   * @param parentAccountId The id of the parent {@link Account} of this container.
   */
  Container(short id, String name, ContainerStatus status, String description, boolean encrypted,
      boolean previouslyEncrypted, boolean cacheable, boolean mediaScanDisabled, String replicationPolicy,
      boolean ttlRequired, boolean securePathRequired, Set<String> contentTypeWhitelistForFilenamesOnDownload,
      boolean backupEnabled, short parentAccountId) {
    checkPreconditions(name, status, encrypted, previouslyEncrypted);
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.cacheable = cacheable;
    this.parentAccountId = parentAccountId;
    switch (currentJsonVersion) {
      case JSON_VERSION_1:
        this.backupEnabled = BACKUP_ENABLED_DEFAULT_VALUE;
        this.encrypted = ENCRYPTED_DEFAULT_VALUE;
        this.previouslyEncrypted = PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;
        this.mediaScanDisabled = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;
        this.replicationPolicy = null;
        this.ttlRequired = TTL_REQUIRED_DEFAULT_VALUE;
        this.securePathRequired = SECURE_PATH_REQUIRED_DEFAULT_VALUE;
        this.contentTypeWhitelistForFilenamesOnDownload =
            CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE;
        break;
      case JSON_VERSION_2:
        this.backupEnabled = backupEnabled;
        this.encrypted = encrypted;
        this.previouslyEncrypted = previouslyEncrypted;
        this.mediaScanDisabled = mediaScanDisabled;
        this.replicationPolicy = replicationPolicy;
        this.ttlRequired = ttlRequired;
        this.securePathRequired = securePathRequired;
        this.contentTypeWhitelistForFilenamesOnDownload =
            contentTypeWhitelistForFilenamesOnDownload == null ? Collections.emptySet()
                : contentTypeWhitelistForFilenamesOnDownload;
        break;
      default:
        throw new IllegalStateException("Unsupported container json version=" + currentJsonVersion);
    }
  }

  /**
   * @return the JSON version to serialize in.
   */
  public static short getCurrentJsonVersion() {
    return currentJsonVersion;
  }

  /**
   * Set the JSON version to serialize in. Note that this is a static setting that will affect all {@link Container}
   * serialization.
   * @param currentJsonVersion the JSON version to serialize in.
   */
  public static void setCurrentJsonVersion(short currentJsonVersion) {
    Container.currentJsonVersion = currentJsonVersion;
  }

  /**
   * Deserializes a {@link JSONObject} to a container object.
   * @param json The {@link JSONObject} to deserialize.
   * @param parentAccountId The ID of the parent {@link Account} of this container. This is passed in because it is
   *                        not always included in the JSON record.
   * @return A container object deserialized from the {@link JSONObject}.
   * @throws JSONException If parsing the {@link JSONObject} fails.
   */
  static Container fromJson(JSONObject json, short parentAccountId) throws JSONException {
    return new Container(json, parentAccountId);
  }

  /**
   * Gets the metadata of the container.
   * @return The metadata of the container.
   * @throws JSONException If fails to compose metadata.
   */
  JSONObject toJson() throws JSONException {
    JSONObject metadata = new JSONObject();
    switch (currentJsonVersion) {
      case JSON_VERSION_1:
        metadata.put(JSON_VERSION_KEY, JSON_VERSION_1);
        metadata.put(CONTAINER_ID_KEY, id);
        metadata.put(CONTAINER_NAME_KEY, name);
        metadata.put(STATUS_KEY, status.name());
        metadata.put(DESCRIPTION_KEY, description);
        metadata.put(IS_PRIVATE_KEY, !cacheable);
        metadata.put(PARENT_ACCOUNT_ID_KEY, parentAccountId);
        break;
      case JSON_VERSION_2:
        metadata.put(Container.JSON_VERSION_KEY, JSON_VERSION_2);
        metadata.put(CONTAINER_ID_KEY, id);
        metadata.put(CONTAINER_NAME_KEY, name);
        metadata.put(Container.STATUS_KEY, status.name());
        metadata.put(DESCRIPTION_KEY, description);
        metadata.put(ENCRYPTED_KEY, encrypted);
        metadata.put(PREVIOUSLY_ENCRYPTED_KEY, previouslyEncrypted);
        metadata.put(CACHEABLE_KEY, cacheable);
        metadata.put(BACKUP_ENABLED_KEY, backupEnabled);
        metadata.put(MEDIA_SCAN_DISABLED_KEY, mediaScanDisabled);
        metadata.putOpt(REPLICATION_POLICY_KEY, replicationPolicy);
        metadata.put(TTL_REQUIRED_KEY, ttlRequired);
        metadata.put(SECURE_PATH_REQUIRED_KEY, securePathRequired);
        if (contentTypeWhitelistForFilenamesOnDownload != null
            && !contentTypeWhitelistForFilenamesOnDownload.isEmpty()) {
          JSONArray contentTypeWhitelistForFilenamesOnDownloadJson = new JSONArray();
          contentTypeWhitelistForFilenamesOnDownload.forEach(contentTypeWhitelistForFilenamesOnDownloadJson::put);
          metadata.put(CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD,
              contentTypeWhitelistForFilenamesOnDownloadJson);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported container json version=" + currentJsonVersion);
    }
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
   * @return {@code true} if blobs in the {@link Container} should be backed up, {@code false} otherwise.
   */
  public boolean isBackupEnabled() {
    return backupEnabled;
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
   * @return the replication policy desired by the container. Can be {@code null} if the container has no preference.
   */
  public String getReplicationPolicy() {
    return replicationPolicy;
  }

  /**
   * @return {@code true} if ttl is required on content created in this container.
   */
  public boolean isTtlRequired() {
    return ttlRequired;
  }

  /**
   * @return the set of content types for which the filename can be sent on download
   */
  public Set<String> getContentTypeWhitelistForFilenamesOnDownload() {
    return contentTypeWhitelistForFilenamesOnDownload;
  }

  /**
   * @return {@code true} if secure path validation is required for url to access blobs in this container.
   */
  public boolean isSecurePathRequired() {
    return securePathRequired;
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
    return id == container.id && encrypted == container.encrypted
        && previouslyEncrypted == container.previouslyEncrypted && cacheable == container.cacheable
        && mediaScanDisabled == container.mediaScanDisabled && parentAccountId == container.parentAccountId
        && Objects.equals(name, container.name) && status == container.status && Objects.equals(description,
        container.description) && Objects.equals(replicationPolicy, container.replicationPolicy)
        && ttlRequired == container.ttlRequired && securePathRequired == container.securePathRequired && Objects.equals(
        contentTypeWhitelistForFilenamesOnDownload, container.contentTypeWhitelistForFilenamesOnDownload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, parentAccountId);
  }

  /**
   * Checks if any required fields is missing for a {@link Container} or for any incompatible settings.
   * @param name The name of the container. Cannot be null.
   * @param status The status of the container. Cannot be null.
   * @param encrypted {@code true} if blobs in the {@link Container} should be encrypted, {@code false} otherwise.
   * @param previouslyEncrypted {@code true} if this {@link Container} was encrypted in the past, or currently, and a
   *                            subset of blobs in it could still be encrypted.
   */
  private void checkPreconditions(String name, ContainerStatus status, boolean encrypted, boolean previouslyEncrypted) {
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
