package com.github.ambry.cloud;

import com.github.ambry.account.CloudReplicationConfig;
import java.io.InputStream;


public interface CloudDestination {

  /**
   * Initialize the destination with the specified configuration string.
   * @param config The configuration to use
   * @throws Exception if initialization encountered an error.
   */
  void initialize(CloudReplicationConfig config) throws Exception;

    /**
     * Upload blob to the cloud destination.
     * @param blobId id of the Ambry blob
     * @param blobSize size of the blob in bytes
     * @param blobInputStream the stream to read blob data
     * @return flag indicating whether the blob was uploaded
     * @throws Exception if an exception occurs
     */
  boolean uploadBlob(String blobId, long blobSize, InputStream blobInputStream) throws Exception;

  /**
   * Delete blob in the cloud destination.
   * @param blobId id of the Ambry blob
   * @return flag indicating whether the blob was deleted
   * @throws Exception if an exception occurs
   */
  boolean deleteBlob(String blobId) throws Exception;
}
