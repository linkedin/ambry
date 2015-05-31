package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.storageservice.BlobStorageService;
import com.github.ambry.storageservice.BlobStorageServiceErrorCode;
import com.github.ambry.storageservice.BlobStorageServiceException;
import com.github.ambry.storageservice.ExecutionData;
import com.github.ambry.storageservice.ExecutionResult;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class AdminBlobStorageService implements BlobStorageService {

  private final ClusterMap clusterMap;

  private boolean up = false;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminBlobStorageService(ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  public void start()
      throws InstantiationException {
    up = true;
  }

  public void shutdown()
      throws Exception {
    up = false;
  }

  public boolean awaitShutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    //nothing to do
    return true;
  }

  public boolean isUp() {
    return up;
  }

  public boolean isTerminated() {
    return !isUp();
  }

  public String putBlob() {
    throw new IllegalStateException("putBlob() not implemented in " + this.getClass().getSimpleName());
  }

  public Object getBlob() {
    throw new IllegalStateException("getBlob() not implemented in " + this.getClass().getSimpleName());
  }

  public boolean deleteBlob() {
    throw new IllegalStateException("deleteBlob not implemented in " + this.getClass().getSimpleName());
  }

  public ExecutionResult execute(ExecutionData executionData)
      throws BlobStorageServiceException {
    if (executionData instanceof AdminExecutionData) {
      AdminOperationType adminOperationType = AdminOperationType.convert(executionData.getOperationType());
      TaskExecutor executor;
      switch (adminOperationType) {
        case Echo:
          executor = new EchoExecutor();
          break;
        case GetReplicasForBlobId:
          executor = new GetReplicasForBlobIdExecutor(clusterMap);
          break;
        case Unknown:
        default:
          throw new BlobStorageServiceException("Received unknown operation type - " + executionData.getOperationType(),
              BlobStorageServiceErrorCode.UnknownOperationType);
      }
      return executor.execute((AdminExecutionData) executionData);
    } else {
      throw new IllegalArgumentException(
          executionData.getClass().getSimpleName() + " object not an instance of AdminExecutionData");
    }
  }
}
