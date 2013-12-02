package com.github.ambry.coordinator;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import static java.lang.Math.min;

/**
 * Interface to encapsulate the logic used to determine which/how error responses of various sorts are necessary to
 * conclude that a blob is not found, deleted, or expired for a get operation.
 */
interface GetResponsePolicy {
  /**
   * Accounts for Blob_Not_Found error responses.
   *
   * @param replicaId of replica that returned Blob_Not_Found error code.
   * @throws CoordinatorException if blob is determined to be not found, deleted, or expired.
   */
  public void blobNotFound(ReplicaId replicaId) throws CoordinatorException;

  /**
   * Accounts for Blob_Deleted error responses.
   *
   * @param replicaId of replica that returned Blob_Deleted error code.
   * @throws CoordinatorException if blob is determined to be not found, deleted, or expired.
   */
  public void blobDeleted(ReplicaId replicaId) throws CoordinatorException;

  /**
   * Accounts for Blob_Expired error responses.
   *
   * @param replicaId of replica that returned Blob_Expired error code.
   * @throws CoordinatorException if blob is determined to be not found, deleted, or expired.
   */
  public void blobExpired(ReplicaId replicaId) throws CoordinatorException;
}


/**
 * Simple threshold based policy for determining when a blob is not found, deleted, or expired.
 */
class GetThresholdResponsePolicy implements GetResponsePolicy {
  private int replicaIdCount;

  private int blobNotFoundCount;
  private int blobDeletedCount;
  private int blobExpiredCount;

  final int Blob_Deleted_Count_Threshold = 1;
  final int Blob_Expired_Count_Threshold = 2;

  public GetThresholdResponsePolicy(PartitionId partitionId) throws CoordinatorException {
    this.replicaIdCount = partitionId.getReplicaIds().size();

    this.blobNotFoundCount = 0;
    this.blobDeletedCount = 0;
    this.blobExpiredCount = 0;
  }

  /**
   * Every replica must return Blob_Not_Found to conclude BlobDoesNotExist.
   */
  @Override
  public void blobNotFound(ReplicaId replicaId) throws CoordinatorException {
    blobNotFoundCount++;
    if (blobNotFoundCount == replicaIdCount) {
      throw new CoordinatorException("Blob not found.", CoordinatorError.BlobDoesNotExist);
    }
  }

  /**
   * A single Blob_Deleted response is sufficient to conclude BlobDeleted.
   */
  @Override
  public void blobDeleted(ReplicaId replicaId) throws CoordinatorException {
    blobDeletedCount++;
    if (blobDeletedCount >= min(Blob_Deleted_Count_Threshold, replicaIdCount)) {
      throw new CoordinatorException("Blob deleted.", CoordinatorError.BlobDeleted);
    }
  }

  /**
   * At least two Blob_Expired responses are necessary to conclude BlobExpired. This protects against a bad clock on an
   * individual server. Corner case are partitions that consist of a single replica and so must make a decision based on
   * a single response.
   */
  @Override
  public void blobExpired(ReplicaId replicaId) throws CoordinatorException {
    blobExpiredCount++;
    if (blobExpiredCount >= min(Blob_Expired_Count_Threshold, replicaIdCount)) {
      throw new CoordinatorException("Blob expired.", CoordinatorError.BlobExpired);
    }
  }
}