package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;

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
  protected long timeToLiveInSeconds;
  protected long creationTimeInMs;

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
         Utils.Infinite_Time);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   */
  public BlobProperties(long blobSize,
                        String serviceId,
                        String ownerId,
                        String contentType,
                        boolean isPrivate) {
    this(blobSize,
         serviceId,
         ownerId,
         contentType,
         isPrivate,
         Utils.Infinite_Time);
  }

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   * @param ownerId The owner of the blob (For example , memberId or groupId)
   * @param contentType The content type of the blob (eg: mime). Can be Null
   * @param isPrivate Is the blob secure
   * @param timeToLiveInSeconds The time to live, in seconds, relative to blob creation time.
   */
  public BlobProperties(long blobSize,
                        String serviceId,
                        String ownerId,
                        String contentType,
                        boolean isPrivate,
                        long timeToLiveInSeconds) {
    this.blobSize = blobSize;
    this.serviceId = serviceId;
    this.ownerId = ownerId;
    this.contentType = contentType;
    this.isPrivate = isPrivate;
    this.timeToLiveInSeconds = timeToLiveInSeconds;
    this.creationTimeInMs = SystemTime.getInstance().milliseconds();
  }

  public long getTimeToLiveInSeconds() {
    return timeToLiveInSeconds;
  }

  public long getBlobSize() {
    return blobSize;
  }

  public boolean isPrivate() {
    return isPrivate;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public String getContentType() {
    return contentType;
  }

  public String getServiceId() {
    return serviceId;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public long getCreationTimeInMs() {
    return creationTimeInMs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlobProperties[");
    sb.append("BlobSize=").append(getBlobSize());
    if(getContentType()!=null) {
      sb.append(", ").append("ContentType=").append(getContentType());
    } else {
      sb.append(", ").append("ContentType=Null");
    }
    if(getOwnerId()!=null) {
      sb.append(", ").append("OwnerId=").append(getOwnerId());
    } else {
      sb.append(", ").append("OwnerId=Null");
    }
    if(getServiceId()!=null) {
      sb.append(", ").append("ServiceId=").append(getServiceId());
    } else {
      sb.append(", ").append("ServiceId=Null");
    }
    sb.append(", ").append("IsPrivate=").append(isPrivate());
    sb.append(", ").append("CreationTimeInMs=").append(getCreationTimeInMs());
    if(getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      sb.append(", ").append("TimeToLiveInSeconds=").append(getTimeToLiveInSeconds());
    } else {
      sb.append(", ").append("TimeToLiveInSeconds=Infinite");
    }
    sb.append("]");
    return sb.toString();
  }
}
