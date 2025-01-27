package com.github.ambry;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.FileMetaDataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileMetaData;
import com.github.ambry.store.FileStore;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReplicaOperationTracker {
  private final ReplicaId replicaId;
  private boolean isRunning =false;
  private final HashMap<String, SegmentOperationTracker> fileToSegmentOperationTrackerMap;
  private RequestInfo requestsToSend;



  private String dataNode;
  private static final Logger logger = LoggerFactory.getLogger(ReplicaOperationTracker.class);
  private FileCopyBasedReplicationState state;

  private final StoreManager storeManager;

  public ReplicaOperationTracker(ReplicaId replicaId, StoreManager storeManager, FileCopyMetrics fileCopyMetrics) {
    this.replicaId = replicaId;
    //requestsToSend = new RequestInfo();
    fileToSegmentOperationTrackerMap = new HashMap<>();
    this.storeManager = storeManager;
  }

  public void start(){
    isRunning = true;

  }

  public boolean isCompleted(){
    return false;
  }

  public RequestInfo pollForRequests(){
    return requestsToSend;
  }

  public void handleMetaDataResponse(ResponseInfo responseInfo) {
    if (state != FileCopyBasedReplicationState.META_DATA_REQUESTED) {
//      logger.error("Remote node: {} Thread name: {} ReplicaMetadataResponse comes back after wrong state {}",
//          remoteDataNode, threadName, state);
      return;
    }

    NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
    //Todo: Log with remoteDataNode
    /**
     * logger.trace(
     *         "Remote node: {} Thread name: {} RemoteReplicaGroup {} ReplicaMetadataResponse come back for correlation id {}, NetworkClientError {}",
     *         responseInfo.getRequestInfo().getRequest()., responseInfo.getRequestInfo().getRequest().getCorrelationId(),
     *         networkClientErrorCode);
     */
    if(networkClientErrorCode == null){
      //update metric
      try{
        DataInputStream dis = new NettyByteBufDataInputStream(responseInfo.content());
        FileMetaDataResponse response = FileMetaDataResponse.readFrom(dis);
        FileStore fileStore = storeManager.getFileStore(replicaId.getPartitionId());
        //fileStore.persistMetaDataToFile(x.getFileMetaData());
        /*if(!fileStore.persistMetaDataToFile()){
          throw new IOException("Failed to persist metadata to file");
          state = FileCopyBasedReplicationState.META_DATA_REQUESTED_RESPONSE_HANDLED;
        }*/



      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void handleChunkResponse(ResponseInfo responseInfo){
    SegmentOperationTracker segmentOperationTracker = fileToSegmentOperationTrackerMap.get(responseInfo.getRequestInfo().getRequest().getCorrelationId());
    if(segmentOperationTracker == null){
      throw new IllegalStateException("SegmentOperationTracker not found for correlationId " + responseInfo.getRequestInfo().getRequest().getCorrelationId());
    }
    segmentOperationTracker.onResponse(responseInfo);
  }


  public void onResponse(ResponseInfo responseInfo){
      RequestOrResponseType type = ((RequestOrResponse) responseInfo.getRequestInfo().getRequest()).getRequestType();
      switch (type) {
        case FileMetaDataRequest:
          handleMetaDataResponse(responseInfo);
          break;
        case FileChunkRequest:
          //handleChunkResponse(responseInfo);
          break;
      }
    }
  public void shutdown(){
    isRunning = false;
  }

  enum FileCopyBasedReplicationState{
    STARTED,
    META_DATA_REQUESTED,
    META_DATA_REQUESTED_RESPONSE_HANDLED,
    META_DATA_REQUESTED_RESPONSE_ERROR,
    CHUNK_REQUESTED,
    CHUNK_REQUESTED_RESPONSE_HANDLED,
    DONE
  }
}
