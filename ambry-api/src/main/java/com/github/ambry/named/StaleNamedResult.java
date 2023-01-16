package com.github.ambry.named;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * Class to convey information about a successful stale named blob pulling {@link NamedBlobDb}.
 */
public class StaleNamedResult {
  private final short accountId;
  private final short containerId;
  private final String blobName;
  private final String blobId;
  private final long version;
  private final Timestamp deleteTs;

  /**
   * @param accountId the account ID for the stale record.
   * @param containerId the container ID for the stale record.
   * @param blobName the blob name for the stale record.
   * @param blobId the blob Id for the stale record.
   * @param version the version for the stale record.
   * @param deleteTs the timestamp of deleting the stale record.
   */
  public StaleNamedResult(short accountId, short containerId, String blobName, String blobId, long version,
      Timestamp deleteTs) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.blobName = blobName;
    this.blobId = blobId;
    this.version = version;
    this.deleteTs = deleteTs;
  }

  /**
   * @return the account Id from the stale record.
   */
  public short getAccountId() {
    return accountId;
  }

  /**
   * @return the container Id from the stale record.
   */
  public short getContainerId() {
    return containerId;
  }

  /**
   * @return the blob Name from the stale record.
   */
  public String getBlobName() {
    return blobName;
  }

  /**
   * @return the blob ID from the stale record.
   */
  public String getBlobId() {
    return blobId;
  }

  /**
   * @return the version from the stale record.
   */
  public long getVersion() {
    return version;
  }

  /**
   * @return the delete timestamp from the stale record.
   */
  public Timestamp getDeleteTs() {
    return deleteTs;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaleNamedResult record = (StaleNamedResult) o;
    return accountId == record.accountId && containerId == record.containerId && Objects.equals(blobName, record.blobName)
        && Objects.equals(blobId, record.blobId) && version == record.version && deleteTs.equals(record.deleteTs);
  }

  @Override
  public String toString() {
    return "StaleNamedResult[accountId=" + accountId + ",containerId=" + containerId + ",blobName=" + blobName +
        ",blobId=" + blobId + ",version=" + version + ",deleteTs=" + deleteTs + "]";
  }
}
