package com.github.ambry.named;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import java.util.List;

public interface PartiallyReadableBlobDb {

  List<PartiallyReadableBlobRecord> get(String accountName, String containerName, String blobName, long chunkOffset)
      throws RestServiceException;

  void put(PartiallyReadableBlobRecord record) throws RestServiceException;

  BlobInfo getBlobInfo(String accountName, String containerName, String blobName, RestRequest restRequest)
      throws RestServiceException;

  void putBlobInfo(String accountName, String containerName, String blobName, long blobSize, String serviceId,
      byte[] userMetadata) throws RestServiceException;

  void updateStatus(String accountName, String containerName, String blobName) throws RestServiceException;
}