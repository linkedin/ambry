/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.Objects;


/**
 * The properties of a blob that the client can set at time of put. The blob size and serviceId are mandatory fields and
 * must be set. The creation time is determined when this object is constructed.
 */
public class BlobProperties {
  private final String serviceId;
  private final String ownerId;
  private final String contentType;
  private final String contentEncoding;
  private final String filename;
  private final boolean isPrivate;
  private final long creationTimeInMs;
  private final short accountId;
  private final short containerId;
  private final boolean isEncrypted;
  private long blobSize;
  private long timeToLiveInSeconds;
  // Non persistent blob properties.
  private final String externalAssetTag;
  private final String reservedMetadataBlobId;

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param accountId accountId of the user who owns the blob
   * @param containerId containerId of the blob
   * @param isEncrypted {@code true} if the blob is encrypted, {@code false} otherwise
   */
  public BlobProperties(long blobSize, String serviceId, short accountId, short containerId, boolean isEncrypted) {
    this(blobSize, serviceId, null, null, false, Utils.Infinite_Time, SystemTime.getInstance().milliseconds(),
        accountId, containerId, isEncrypted, null, null, null, null);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param accountId accountId of the user who owns the blob
   * @param containerId containerId of the blob
   * @param isEncrypted {@code true} if the blob is encrypted, {@code false} otherwise
   * @param creationTimeInMs The time at which the blob is created.
   */
  public BlobProperties(long blobSize, String serviceId, short accountId, short containerId, boolean isEncrypted,
      long creationTimeInMs) {
    this(blobSize, serviceId, null, null, false, Utils.Infinite_Time, creationTimeInMs, accountId, containerId,
        isEncrypted, null, null, null, null);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   * @param timeToLiveInSeconds The time to live, in seconds, relative to blob creation time.
   * @param accountId accountId of the user who owns the blob
   * @param containerId containerId of the blob
   * @param isEncrypted whether this blob is encrypted.
   * @param externalAssetTag externalAssetTag for this blob. This is a non-persistent field.
   * @param contentEncoding the field to identify if the blob is compressed.
   * @param filename the name of the file.
   */
  public BlobProperties(long blobSize, String serviceId, String ownerId, String contentType, boolean isPrivate,
      long timeToLiveInSeconds, short accountId, short containerId, boolean isEncrypted, String externalAssetTag,
      String contentEncoding, String filename) {
    this(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
        SystemTime.getInstance().milliseconds(), accountId, containerId, isEncrypted, externalAssetTag, contentEncoding,
        filename, null);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   * @param timeToLiveInSeconds The time to live, in seconds, relative to blob creation time.
   * @param accountId accountId of the user who owns the blob
   * @param containerId containerId of the blob
   * @param isEncrypted whether this blob is encrypted.
   * @param externalAssetTag externalAssetTag for this blob. This is a non-persistent field.
   * @param contentEncoding the field to identify if the blob is compressed.
   * @param reservedMetadataBlobId the reserved metadata blob id for chunked uploads or stitched blobs. Can be {@code null}.
   * @param filename the name of the file.
   */
  public BlobProperties(long blobSize, String serviceId, String ownerId, String contentType, boolean isPrivate,
      long timeToLiveInSeconds, short accountId, short containerId, boolean isEncrypted, String externalAssetTag,
      String contentEncoding, String filename, String reservedMetadataBlobId) {
    this(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
        SystemTime.getInstance().milliseconds(), accountId, containerId, isEncrypted, externalAssetTag, contentEncoding,
        filename, reservedMetadataBlobId);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   * @param timeToLiveInSeconds The time to live, in seconds, relative to blob creation time.
   * @param creationTimeInMs The time at which the blob is created.
   * @param accountId accountId of the user who owns the blob
   * @param containerId containerId of the blob
   * @param isEncrypted whether this blob is encrypted.
   * @param externalAssetTag externalAssetTag for this blob. This is a non-persistent field.
   * @param contentEncoding the field to identify if the blob is compressed.
   * @param filename the name of the file.
   * @param reservedMetadataBlobId the reserved metadata blob id for chunked uploads or stitched blobs. Can be {@code null}.
   */
  public BlobProperties(long blobSize, String serviceId, String ownerId, String contentType, boolean isPrivate,
      long timeToLiveInSeconds, long creationTimeInMs, short accountId, short containerId, boolean isEncrypted,
      String externalAssetTag, String contentEncoding, String filename, String reservedMetadataBlobId) {
    this.blobSize = blobSize;
    this.serviceId = serviceId;
    this.ownerId = ownerId;
    this.contentType = contentType;
    this.isPrivate = isPrivate;
    this.creationTimeInMs = creationTimeInMs;
    this.timeToLiveInSeconds = timeToLiveInSeconds;
    this.accountId = accountId;
    this.containerId = containerId;
    this.isEncrypted = isEncrypted;
    this.externalAssetTag = externalAssetTag;
    this.contentEncoding = contentEncoding;
    this.filename = filename;
    this.reservedMetadataBlobId = reservedMetadataBlobId;
  }

  /**
   * Constructor to copy all fields from another {@link BlobProperties}.
   * @param other the {@link BlobProperties} to copy.
   */
  public BlobProperties(BlobProperties other) {
    this(other.blobSize, other.serviceId, other.ownerId, other.contentType, other.isPrivate, other.timeToLiveInSeconds,
        other.creationTimeInMs, other.accountId, other.containerId, other.isEncrypted, other.externalAssetTag,
        other.contentEncoding, other.filename, other.reservedMetadataBlobId);
  }

  public long getTimeToLiveInSeconds() {
    return timeToLiveInSeconds;
  }

  public long getBlobSize() {
    return blobSize;
  }

  /**
   * This should only be used to determine the "virtual container" for blobs with V1 IDs.
   * @return whether the blob was private at creation time.
   */
  @Deprecated
  public boolean isPrivate() {
    return isPrivate;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public String getContentType() {
    return contentType;
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public String getFilename() {
    return filename;
  }

  /**
   * ServiceId of the uploader of the blob
   * @return the serviceId of the uploader of the blob
   */
  public String getServiceId() {
    return serviceId;
  }

  public long getCreationTimeInMs() {
    return creationTimeInMs;
  }

  public short getAccountId() {
    return accountId;
  }

  public short getContainerId() {
    return containerId;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  /**
   * @return the ExternalAssetTag of a uploaded blob. Can be null.
   */
  public String getExternalAssetTag() {
    return externalAssetTag;
  }

  /**
   * @return the reserved metadata blob id for chunked uploads or stitched blobs. Can be {@code null}.
   */
  public String getReservedMetadataBlobId() {
    return reservedMetadataBlobId;
  }

  /**
   * @param timeToLiveInSeconds the new value of timeToLiveInSeconds
   */
  public void setTimeToLiveInSeconds(long timeToLiveInSeconds) {
    this.timeToLiveInSeconds = timeToLiveInSeconds;
  }

  /**
   * @param blobSize the new blob size in bytes
   */
  public void setBlobSize(long blobSize) {
    this.blobSize = blobSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlobProperties[");
    sb.append("BlobSize=").append(getBlobSize());
    sb.append(", ").append("ContentType=").append(getContentType());
    sb.append(", ").append("OwnerId=").append(getOwnerId());
    sb.append(", ").append("ServiceId=").append(getServiceId());
    sb.append(", ").append("IsPrivate=").append(isPrivate());
    sb.append(", ").append("CreationTimeInMs=").append(getCreationTimeInMs());
    if (getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      sb.append(", ").append("TimeToLiveInSeconds=").append(getTimeToLiveInSeconds());
    } else {
      sb.append(", ").append("TimeToLiveInSeconds=Infinite");
    }
    sb.append(", ").append("AccountId=").append(getAccountId());
    sb.append(", ").append("ContainerId=").append(getContainerId());
    sb.append(", ").append("IsEncrypted=").append(isEncrypted());
    sb.append(", ").append("externalAssetTag=").append(getExternalAssetTag());
    sb.append(", ").append("ContentEncoding=").append(getContentEncoding());
    sb.append(", ").append("Filename=").append(getFilename());
    sb.append(", ")
        .append("ReservedMetadataBlobId=")
        .append(nullOrEmptry(getReservedMetadataBlobId())? "null" : getReservedMetadataBlobId());
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlobProperties that = (BlobProperties) o;
    return isPrivate == that.isPrivate && creationTimeInMs == that.creationTimeInMs && accountId == that.accountId
        && containerId == that.containerId && isEncrypted == that.isEncrypted && blobSize == that.blobSize
        && timeToLiveInSeconds == that.timeToLiveInSeconds && Objects.equals(serviceId, that.serviceId)
        && Objects.equals(ownerId, that.ownerId) && Objects.equals(contentType, that.contentType) && Objects.equals(
        contentEncoding, that.contentEncoding) && Objects.equals(filename, that.filename) && Objects.equals(
        externalAssetTag, that.externalAssetTag) && Objects.equals(reservedMetadataBlobId, that.reservedMetadataBlobId);
  }

  /**
   * If the string is null or empty, return true.
   * @param str string to check
   * @return true if the string is either null or is an empty string.
   */
  private boolean nullOrEmptry(String str) {
    return str == null || str.isEmpty();
  }

  /**
   * check if two BlobProperties are equal.
   * contentEquals treats null same as empty string "". It applied to ownerId and contentType.
   * @return true if from the content point of view, two {@link BlobProperties} are the same.
   */
  public boolean contentEquals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlobProperties that = (BlobProperties) o;
    return isPrivate == that.isPrivate
        && creationTimeInMs == that.creationTimeInMs
        && accountId == that.accountId
        && containerId == that.containerId
        && isEncrypted == that.isEncrypted
        && blobSize == that.blobSize
        && timeToLiveInSeconds == that.timeToLiveInSeconds
        && Objects.equals(serviceId, that.serviceId)
        && (Objects.equals(ownerId, that.ownerId) || (nullOrEmptry(ownerId) && nullOrEmptry(that.ownerId)))
        && (Objects.equals(contentType, that.contentType) || (nullOrEmptry(contentType) && nullOrEmptry(that.contentType)))
        && Objects.equals(contentEncoding, that.contentEncoding)
        && Objects.equals(filename, that.filename)
        && Objects.equals(externalAssetTag, that.externalAssetTag)
        && Objects.equals(reservedMetadataBlobId, that.reservedMetadataBlobId);
  }
}
