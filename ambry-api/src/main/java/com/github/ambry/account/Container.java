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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;


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
@JsonDeserialize(builder = ContainerBuilder.class)
public class Container {

  // constants
  static final String JSON_VERSION_KEY = "version";
  static final String CONTAINER_NAME_KEY = "containerName";
  static final String CONTAINER_ID_KEY = "containerId";
  static final String PREVIOUSLY_ENCRYPTED_KEY = "previouslyEncrypted";
  static final String OVERRIDE_ACCOUNT_ACL_KEY = "overrideAccountAcl";

  static final boolean BACKUP_ENABLED_DEFAULT_VALUE = false;
  static final boolean ENCRYPTED_DEFAULT_VALUE = false;
  static final boolean PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE = ENCRYPTED_DEFAULT_VALUE;
  static final boolean MEDIA_SCAN_DISABLED_DEFAULT_VALUE = false;
  static final boolean TTL_REQUIRED_DEFAULT_VALUE = true;
  static final long CONTAINER_DELETE_TRIGGER_TIME_DEFAULT_VALUE = 0;
  static final boolean SECURE_PATH_REQUIRED_DEFAULT_VALUE = false;
  static final boolean OVERRIDE_ACCOUNT_ACL_DEFAULT_VALUE = false;
  static final NamedBlobMode NAMED_BLOB_MODE_DEFAULT_VALUE = NamedBlobMode.DISABLED;
  static final boolean CACHEABLE_DEFAULT_VALUE = true;
  static final Set<String> CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE = Collections.emptySet();
  static final long LAST_MODIFIED_TIME_DEFAULT_VALUE = 0;
  static final int SNAPSHOT_VERSION_DEFAULT_VALUE = 0;
  static final String ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT_VALUE = "";
  static final Long CACHE_TTL_IN_SECOND_DEFAULT_VALUE = null;
  static final Set<String> USER_METADATA_KEYS_TO_NOT_PREFIX_IN_RESPONSE_DEFAULT_VALUE = Collections.emptySet();

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
   * The delete trigger time of {@link #UNKNOWN_CONTAINER}
   */
  public static final long UNKNOWN_CONTAINER_DELETE_TRIGGER_TIME = CONTAINER_DELETE_TRIGGER_TIME_DEFAULT_VALUE;

  /**
   * The delete trigger time of {@link #DEFAULT_PUBLIC_CONTAINER}
   */
  public static final long DEFAULT_PUBLIC_CONTAINER_DELETE_TRIGGER_TIME = CONTAINER_DELETE_TRIGGER_TIME_DEFAULT_VALUE;

  /**
   * The delete trigger time of {@link #DEFAULT_PRIVATE_CONTAINER}
   */
  public static final long DEFAULT_PRIVATE_CONTAINER_DELETE_TRIGGER_TIME = CONTAINER_DELETE_TRIGGER_TIME_DEFAULT_VALUE;

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
          BACKUP_ENABLED_DEFAULT_VALUE, OVERRIDE_ACCOUNT_ACL_DEFAULT_VALUE, NAMED_BLOB_MODE_DEFAULT_VALUE,
          UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID, UNKNOWN_CONTAINER_DELETE_TRIGGER_TIME, LAST_MODIFIED_TIME_DEFAULT_VALUE,
          SNAPSHOT_VERSION_DEFAULT_VALUE, ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT_VALUE, CACHE_TTL_IN_SECOND_DEFAULT_VALUE,
          USER_METADATA_KEYS_TO_NOT_PREFIX_IN_RESPONSE_DEFAULT_VALUE);

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
          BACKUP_ENABLED_DEFAULT_VALUE, OVERRIDE_ACCOUNT_ACL_DEFAULT_VALUE, NAMED_BLOB_MODE_DEFAULT_VALUE,
          DEFAULT_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID, DEFAULT_PRIVATE_CONTAINER_DELETE_TRIGGER_TIME,
          LAST_MODIFIED_TIME_DEFAULT_VALUE, SNAPSHOT_VERSION_DEFAULT_VALUE, ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT_VALUE,
          CACHE_TTL_IN_SECOND_DEFAULT_VALUE, USER_METADATA_KEYS_TO_NOT_PREFIX_IN_RESPONSE_DEFAULT_VALUE);

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
          BACKUP_ENABLED_DEFAULT_VALUE, OVERRIDE_ACCOUNT_ACL_DEFAULT_VALUE, NAMED_BLOB_MODE_DEFAULT_VALUE,
          DEFAULT_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID, DEFAULT_PUBLIC_CONTAINER_DELETE_TRIGGER_TIME,
          LAST_MODIFIED_TIME_DEFAULT_VALUE, SNAPSHOT_VERSION_DEFAULT_VALUE, ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT_VALUE,
          CACHE_TTL_IN_SECOND_DEFAULT_VALUE, USER_METADATA_KEYS_TO_NOT_PREFIX_IN_RESPONSE_DEFAULT_VALUE);

  // container field variables
  @JsonProperty(CONTAINER_ID_KEY)
  private final short id;
  @JsonProperty(CONTAINER_NAME_KEY)
  private final String name;
  private final ContainerStatus status;
  private final long deleteTriggerTime;
  private final String description;
  private final boolean encrypted;
  @JsonProperty(PREVIOUSLY_ENCRYPTED_KEY)
  private final boolean previouslyEncrypted;
  private final boolean cacheable;
  private final boolean backupEnabled;
  private final boolean mediaScanDisabled;
  private final String replicationPolicy;
  private final boolean ttlRequired;
  private final boolean securePathRequired;
  @JsonProperty(OVERRIDE_ACCOUNT_ACL_KEY)
  private final boolean overrideAccountAcl;
  private final NamedBlobMode namedBlobMode;
  private final String accessControlAllowOrigin;
  private final Set<String> contentTypeWhitelistForFilenamesOnDownload;
  private final short parentAccountId;
  private final long lastModifiedTime;
  private final int snapshotVersion;
  private final Long cacheTtlInSecond;
  private final Set<String> userMetadataKeysToNotPrefixInResponse;
  @JsonProperty(JSON_VERSION_KEY)
  private final int version = JSON_VERSION_2; // the default version is 2

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
   *                                                   download.
   * @param backupEnabled Whether backup is enabled for this container or not.
   * @param overrideAccountAcl Whether to override account-level ACLs.
   * @param namedBlobMode how named blob requests should be treated for this container.
   * @param parentAccountId The id of the parent {@link Account} of this container.
   * @param lastModifiedTime created/modified time of this container.
   * @param accessControlAllowOrigin The Access-Control-Allow-Origin header field name of this container.
   */
  Container(short id, String name, ContainerStatus status, String description, boolean encrypted,
      boolean previouslyEncrypted, boolean cacheable, boolean mediaScanDisabled, String replicationPolicy,
      boolean ttlRequired, boolean securePathRequired, Set<String> contentTypeWhitelistForFilenamesOnDownload,
      boolean backupEnabled, boolean overrideAccountAcl, NamedBlobMode namedBlobMode, short parentAccountId,
      long deleteTriggerTime, long lastModifiedTime, int snapshotVersion, String accessControlAllowOrigin,
      Long cacheTtlInSecond, Set<String> userMetadataKeysToNotPrefixInResponse) {
    checkPreconditions(name, status, encrypted, previouslyEncrypted);
    this.id = id;
    this.name = name;
    this.status = status;
    this.description = description;
    this.cacheable = cacheable;
    this.parentAccountId = parentAccountId;
    this.lastModifiedTime = lastModifiedTime;
    this.snapshotVersion = snapshotVersion;
    switch (currentJsonVersion) {
      case JSON_VERSION_1:
        this.backupEnabled = BACKUP_ENABLED_DEFAULT_VALUE;
        this.encrypted = ENCRYPTED_DEFAULT_VALUE;
        this.previouslyEncrypted = PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE;
        this.mediaScanDisabled = MEDIA_SCAN_DISABLED_DEFAULT_VALUE;
        this.replicationPolicy = null;
        this.deleteTriggerTime = CONTAINER_DELETE_TRIGGER_TIME_DEFAULT_VALUE;
        this.ttlRequired = TTL_REQUIRED_DEFAULT_VALUE;
        this.securePathRequired = SECURE_PATH_REQUIRED_DEFAULT_VALUE;
        this.contentTypeWhitelistForFilenamesOnDownload =
            CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE;
        this.overrideAccountAcl = OVERRIDE_ACCOUNT_ACL_DEFAULT_VALUE;
        this.namedBlobMode = NAMED_BLOB_MODE_DEFAULT_VALUE;
        this.accessControlAllowOrigin = ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT_VALUE;
        this.cacheTtlInSecond = CACHE_TTL_IN_SECOND_DEFAULT_VALUE;
        this.userMetadataKeysToNotPrefixInResponse = USER_METADATA_KEYS_TO_NOT_PREFIX_IN_RESPONSE_DEFAULT_VALUE;
        break;
      case JSON_VERSION_2:
        this.backupEnabled = backupEnabled;
        this.encrypted = encrypted;
        this.previouslyEncrypted = previouslyEncrypted;
        this.mediaScanDisabled = mediaScanDisabled;
        this.replicationPolicy = replicationPolicy;
        this.deleteTriggerTime = deleteTriggerTime;
        this.ttlRequired = ttlRequired;
        this.securePathRequired = securePathRequired;
        this.contentTypeWhitelistForFilenamesOnDownload =
            contentTypeWhitelistForFilenamesOnDownload == null ? Collections.emptySet()
                : contentTypeWhitelistForFilenamesOnDownload;
        this.overrideAccountAcl = overrideAccountAcl;
        this.namedBlobMode = namedBlobMode;
        this.accessControlAllowOrigin = accessControlAllowOrigin;
        this.cacheTtlInSecond = cacheTtlInSecond;
        this.userMetadataKeysToNotPrefixInResponse =
            userMetadataKeysToNotPrefixInResponse == null ? Collections.emptySet()
                : userMetadataKeysToNotPrefixInResponse;
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
   * Check if passed in container is same with current one.
   * @param containerToCompare the {@link Container} to compare.
   * @return {@code true} if two containers are equivalent. {@code false} otherwise.
   */
  boolean isSameContainer(Container containerToCompare) {
    //@formatter:off
    return Objects.equals(this.isCacheable(), containerToCompare.isCacheable())
        && Objects.equals(this.isEncrypted(), containerToCompare.isEncrypted())
        && Objects.equals(this.isMediaScanDisabled(), containerToCompare.isMediaScanDisabled())
        && Objects.equals(this.isTtlRequired(), containerToCompare.isTtlRequired())
        && Objects.equals(this.getStatus(), containerToCompare.getStatus())
        && Objects.equals(this.getReplicationPolicy(), containerToCompare.getReplicationPolicy())
        && Objects.equals(this.isSecurePathRequired(), containerToCompare.isSecurePathRequired())
        && Objects.equals(this.isBackupEnabled(), containerToCompare.isBackupEnabled())
        && Objects.equals(this.getContentTypeWhitelistForFilenamesOnDownload(), containerToCompare.getContentTypeWhitelistForFilenamesOnDownload())
        && Objects.equals(this.isAccountAclOverridden(), containerToCompare.isAccountAclOverridden());
    //@formatter:on
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
   * Gets the delete trigger time of the container.
   * @return The delete trigger time of the container.
   */
  public Long getDeleteTriggerTime() {
    return deleteTriggerTime;
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
   * @return {@code true} if current container overrides parent account's ACL.
   */
  @JsonIgnore
  public boolean isAccountAclOverridden() {
    return overrideAccountAcl;
  }

  /**
   * @return how named blob requests should be treated for this container.
   */
  public NamedBlobMode getNamedBlobMode() {
    return namedBlobMode;
  }

  /**
   * @return The Access-Control-Allow-Origin header field name for this container.
   */
  public String getAccessControlAllowOrigin() {
    return accessControlAllowOrigin;
  }

  /**
   * @return The ttl in second for cache settings. If the returned value is null, then caller is free to use whatever
   * value caller deems reasonable.
   */
  public Long getCacheTtlInSecond() {
    return cacheTtlInSecond;
  }

  /**
   * @return A set of user metadata keys to not prefix with usermetadata prefix in response header.
   */
  public Set<String> getUserMetadataKeysToNotPrefixInResponse() {
    return userMetadataKeysToNotPrefixInResponse;
  }

  /**
   * Gets the if of the {@link Account} that owns this container.
   * @return The id of the parent {@link Account} of this container.
   */
  @JsonIgnore
  public short getParentAccountId() {
    return parentAccountId;
  }

  /**
   * Get the created/modified time of this Container
   * @return epoch time in milliseconds
   */
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * The snapshot version is generally the number of modifications to the container that were expected to have occurred
   * before the current time. This is used to validate that there were no unexpected container modifications that could be
   * inadvertently overwritten by an container update.
   * @return the expected version for the container record.
   */
  public int getSnapshotVersion() {
    return snapshotVersion;
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
    // Note: do not under any circumstances compare snapshotVersion!
    //@formatter:off
    return id == container.id
        && encrypted == container.encrypted
        && previouslyEncrypted == container.previouslyEncrypted
        && cacheable == container.cacheable
        && mediaScanDisabled == container.mediaScanDisabled
        && parentAccountId == container.parentAccountId
        && Objects.equals(name, container.name)
        && status == container.status
        && deleteTriggerTime == container.deleteTriggerTime
        && Objects.equals(description, container.description)
        && Objects.equals(replicationPolicy, container.replicationPolicy)
        && ttlRequired == container.ttlRequired
        && securePathRequired == container.securePathRequired
        && overrideAccountAcl == container.overrideAccountAcl
        && namedBlobMode == container.namedBlobMode
        && Objects.equals(accessControlAllowOrigin, container.accessControlAllowOrigin)
        && Objects.equals(contentTypeWhitelistForFilenamesOnDownload, container.contentTypeWhitelistForFilenamesOnDownload)
        && Objects.equals(cacheTtlInSecond, container.cacheTtlInSecond)
        && Objects.equals(userMetadataKeysToNotPrefixInResponse, container.userMetadataKeysToNotPrefixInResponse);
    //@formatter:on
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
   * Status of the container. {@code ACTIVE} means this container is in operational state, {@code INACTIVE} means
   * the container has been deactivated, and {@code DELETE_IN_PROGRESS} means blobs in this container are being
   * deleted and no active ACL.
   */
  public enum ContainerStatus {
    ACTIVE, INACTIVE, DELETE_IN_PROGRESS
  }

  /**
   * How named blob requests should be treated for this container.
   */
  public enum NamedBlobMode {
    /**
     * Named blob APIs are completely disabled for this container.
     */
    DISABLED,

    /**
     * Both named blob APIs and blob ID APIs may be used for this container, existing named blob can be updated
     */
    OPTIONAL,

    /**
     * Both named blob APIs and blob ID APIs may be used for this container, do not allow updating named blob
     */
    NO_UPDATE
  }
}
