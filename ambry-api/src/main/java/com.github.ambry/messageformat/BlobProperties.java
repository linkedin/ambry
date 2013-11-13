package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;

/**
 * The set of properties that the client can set
 */
public class BlobProperties {
  protected long timeToLiveInMs;
  protected boolean isPrivate;
  protected String contentType;
  protected String memberId;
  protected String parentBlobId;
  protected long blobSize;
  protected String serviceId;
  protected long creationTimeInMs;

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   */
  public BlobProperties(long blobSize, String serviceId) {
    // default set to 1 hour
    timeToLiveInMs = SystemTime.getInstance().milliseconds() + (SystemTime.MsPerSec * SystemTime.SecsPerHour);
    isPrivate = false;
    this.blobSize = blobSize;
    this.creationTimeInMs = SystemTime.getInstance().milliseconds();
    this.serviceId = serviceId;
    this.contentType = null;
    this.memberId = null;
    this.parentBlobId = null;
  }

  /**
   * @param timeToLiveInMs The System time in ms when the blob needs to be deleted
   * @param isPrivate Is the blob secure
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param memberId The user who created this blob. Can be Null
   * @param parentBlobId If this blob was generated from another blob, the id needs to be specified. Null otherwise
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   */
  public BlobProperties(long timeToLiveInMs, boolean isPrivate, String contentType, String memberId,
                        String parentBlobId, long blobSize, String serviceId) {
    this.timeToLiveInMs = timeToLiveInMs;
    this.isPrivate = isPrivate;
    this.contentType = contentType;
    this.memberId = memberId;
    this.parentBlobId = parentBlobId;
    this.blobSize = blobSize;
    this.serviceId = serviceId;
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

  public void setMemberId(String id) {
    memberId = id;
  }

  public String getMemberId() {
    return memberId;
  }

  public void setParentBlobId(String parentBlobId) {
    this.parentBlobId = parentBlobId;
  }

  public String getParentBlobId() {
    return parentBlobId;
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

  public long getCreationTimeInMs() {
    return creationTimeInMs;
  }
}
