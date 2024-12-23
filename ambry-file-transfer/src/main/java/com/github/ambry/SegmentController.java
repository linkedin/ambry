package com.github.ambry;

import com.github.ambry.network.RequestInfo;
import java.io.File;


public class SegmentController {
  private final SegmentOperationTracker segmentOperationTracker;
  private final ReplicaOperationTracker replicaOperationTracker;

  private final String dataDir;

  public SegmentController(SegmentOperationTracker segmentOperationTracker, ReplicaOperationTracker replicaOperationTracker) {
    this.segmentOperationTracker = segmentOperationTracker;
    this.replicaOperationTracker = replicaOperationTracker;
    this.dataDir = "";
  }

  public void start() {
    replicaOperationTracker.start();
  }

  public boolean isCompleted() {
    return replicaOperationTracker.isCompleted();
  }

  //public boolean write

  public boolean createNewFile(String dataDir, String fileName){
    File newFile= new File(dataDir, fileName);
    try {
      return newFile.createNewFile();
    } catch (Exception e) {
      return false;
    }
  }

  public RequestInfo pollForRequests() {
    return replicaOperationTracker.pollForRequests();
  }

  public void handleMetaDataResponse(ResponseInfo responseInfo) {
    replicaOperationTracker.handleMetaDataResponse(responseInfo);
  }
}
