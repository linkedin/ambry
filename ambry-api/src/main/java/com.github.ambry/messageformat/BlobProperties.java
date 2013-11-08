package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;

/**
 * The set of properties that the client can set
 */
public class BlobProperties {
  private long timeToLive;
  private boolean isPrivate;
  private String contentType;
  private String memberId;
  private String parentBlobId;
  private long blobSize;
  private String serviceId;
  private long creationTime;

  public BlobProperties(long blobSize, String serviceId) {
    timeToLive = SystemTime.getInstance().milliseconds() + (SystemTime.SecsPerHour * SystemTime.MsPerSec);
    isPrivate = false;
    this.blobSize = blobSize;
    this.creationTime = SystemTime.getInstance().milliseconds();
    this.serviceId = serviceId;
    this.contentType = null;
    this.memberId = null;
    this.parentBlobId = null;
  }

  public BlobProperties(long timeToLive, boolean isPrivate, String contentType, String memberId,
                        String parentBlobId, long blobSize, String serviceId, long creationTime) {
    this.timeToLive = timeToLive;
    this.isPrivate = isPrivate;
    this.contentType = contentType;
    this.memberId = memberId;
    this.parentBlobId = parentBlobId;
    this.blobSize = blobSize;
    this.serviceId = serviceId;
    this.creationTime = creationTime;
  }

  public void setTimeToLive(long timeToLive) {
    this.timeToLive = timeToLive;
  }

  public long getTimeToLive() {
    return timeToLive;
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

  public long getCreationTime() {
    return creationTime;
  }
}
