package com.github.ambry.named;

import com.github.ambry.rest.RestServiceException;
import java.util.List;


public interface PartiallyReadableBlobDb {

  List<PartiallyReadableBlobRecord> get(String accountName, String containerName, String blobName, long chunkOffset)
      throws RestServiceException;

  void put(PartiallyReadableBlobRecord record) throws RestServiceException;
}
