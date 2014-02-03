package com.github.ambry.messageformat;

import com.github.ambry.utils.SystemTime;

/**
 * The properties of a blob that the client can set at time of put. The blob size and serviceId are mandatory fields and
 * must be set. The creation time is determined when this object is constructed.
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

  public static final long Infinite_TTL = -1;
  /**
   * The size in bytes of the largest blob that can be stored.
   */
  // TODO: Determine safe value for max user metadata size in bytes.
  /**
   * The size in bytes of the largest blob user metadata that can be stored.
   */
  public static final long Max_Blob_User_Metadata_Size_In_Bytes = 64 * 1024;
  public static final long Max_Blob_Size_In_Bytes = 20 * 1024 * 1024;

  /**
   * @param blobSize The size of the blob in bytes
   * @param serviceId The service id that is creating this blob
   */
  public BlobProperties(long blobSize, String serviceId) {
    this(Infinite_TTL,
         false, // public
         null,
         null,
         null,
         blobSize,
         serviceId);
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
    this.blobSize = verifiedBlobSize(blobSize);
    this.serviceId = serviceId;
    this.creationTimeInMs = SystemTime.getInstance().milliseconds();
  }

  private long verifiedBlobSize(long blobSize) {
    if (blobSize >= Max_Blob_Size_In_Bytes) {
      throw new IllegalArgumentException("Specified Blob size is too large. Max Blob size allowed is " +
                                         Max_Blob_Size_In_Bytes + " bytes.");
    }
    if (blobSize < 0) {
      throw new IllegalArgumentException("Specified Blob size is negative. Must be positive.");
    }
    return blobSize;
  }

  public void setTimeToLiveInMs(long timeToLiveInMs) {
    this.timeToLiveInMs = timeToLiveInMs;
  }

  public long getTimeToLiveInMs() {
    return timeToLiveInMs;
  }

  public void setBlobSize(long blobSize) {
    this.blobSize = verifiedBlobSize(blobSize);
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
