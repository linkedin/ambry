package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;

/**
 * The properties of a blob that the client can set at time of put. The blob size and serviceId are mandatory fields and
 * must be set. The creation time is determined when this object is constructed.
 */
public class BlobProperties {

  protected long blobSize;
  protected String serviceId;
  protected String ownerId;
  protected String contentType;
  protected boolean isPrivate;
  protected long timeToLiveInMs;
  protected int userMetadataSize;
  protected long creationTimeInMs;


  public static final long Infinite_TTL = -1;

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   */
  public BlobProperties(long blobSize, String serviceId) {
    this(blobSize,
            serviceId,
            null,
            null,
            false,
            0,
            Infinite_TTL);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   * @param usermetadataSize The size of the usermetadata stored with this blob.
   *                         It is zero if there is no usermetadata
   */
  public BlobProperties(long blobSize,
                        String serviceId,
                        String ownerId,
                        String contentType,
                        boolean isPrivate,
                        int usermetadataSize) {
    this(blobSize,
            serviceId,
            ownerId,
            contentType,
            isPrivate,
            usermetadataSize,
            Infinite_TTL);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   * @param usermetadataSize The size of the usermetadata stored with this blob.
   *                         It is zero if there is no usermetadata
   * @param timeToLiveInMs The System time in ms when the blob needs to be deleted
   */
  public BlobProperties(long blobSize,
                        String serviceId,
                        String ownerId,
                        String contentType,
                        boolean isPrivate,
                        int usermetadataSize,
                        long timeToLiveInMs) {
    this.blobSize = blobSize;
    this.serviceId = serviceId;
    this.ownerId = ownerId;
    this.contentType = contentType;
    this.isPrivate = isPrivate;
    this.timeToLiveInMs = timeToLiveInMs;
    this.userMetadataSize = usermetadataSize;
    this.creationTimeInMs = SystemTime.getInstance().milliseconds();
  }

  public void setTimeToLiveInMs(long timeToLiveInMs) {
    this.timeToLiveInMs = timeToLiveInMs;
  }

  public long getTimeToLiveInMs() {
    return timeToLiveInMs;
  }

  public void setBlobSize(long blobSize) {
    this.blobSize = blobSize;
  }

  public long getBlobSize() {
    return blobSize;
  }

  public void setPrivate() {
    isPrivate = true;
  }

  public boolean isPrivate() {
    return isPrivate;
  }

  public void setOwnerId(String id) {
    ownerId = id;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public String getContentType() {
    return contentType;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public String getServiceId() {
    return serviceId;
  }

  public void setUserMetadataSize(int usermetadataSize) {
    this.userMetadataSize = usermetadataSize;
  }

  public int getUserMetadataSize() {
    return userMetadataSize;
  }

  public long getCreationTimeInMs() {
    return creationTimeInMs;
  }
}