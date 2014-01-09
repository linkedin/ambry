package com.github.ambry.server;

import java.io.DataInputStream;

import com.github.ambry.clustermap.*;
import com.github.ambry.messageformat.*;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.shared.*;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */

public class AmbryRequests implements RequestAPI {

  private StoreManager storeManager;
  private final RequestResponseChannel requestResponseChannel;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final ClusterMap clusterMap;
  private final DataNodeId currentNode;

  public AmbryRequests(StoreManager storeManager,
                       RequestResponseChannel requestResponseChannel,
                       ClusterMap clusterMap,
                       DataNodeId nodeId) {
    this.storeManager = storeManager;
    this.requestResponseChannel = requestResponseChannel;
    this.clusterMap = clusterMap;
    this.currentNode = nodeId;
  }

  public void handleRequests(Request request) throws InterruptedException {
    try {
      // log
      DataInputStream stream = new DataInputStream(request.getInputStream());
      RequestResponseType type = RequestResponseType.values()[stream.readShort()];
      switch (type) {
        case PutRequest:
          handlePutRequest(request);
          break;
        case GetRequest:
          handleGetRequest(request);
          break;
        case DeleteRequest:
          handleDeleteRequest(request);
          break;
        case TTLRequest:
          handleTTLRequest(request);
          break;
        default: throw new UnsupportedOperationException("Request type not supported");
      }
    }
    catch (Exception e) {
      logger.error("Error while handling request {}. Closing connection", e);
      requestResponseChannel.closeConnection(request);
    }
  }

  public void handlePutRequest(Request request) throws IOException, InterruptedException {
    PutRequest putRequest = PutRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);

    try {
      ServerErrorCode error = validateRequest(putRequest.getBlobId().getPartition(), true);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating put request failed with error {}", error);
        PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                putRequest.getClientId(),
                error);
        requestResponseChannel.sendResponse(response, request);
      }
      MessageFormatInputStream stream = new PutMessageFormatInputStream(putRequest.getBlobId(),
                                                                        putRequest.getBlobProperties(),
                                                                        putRequest.getUsermetadata(),
                                                                        putRequest.getData(),
                                                                        putRequest.getBlobProperties().getBlobSize());
      MessageInfo info = new MessageInfo(putRequest.getBlobId(),
                                         stream.getSize(),
                                         putRequest.getBlobProperties().getTimeToLiveInMs());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      Store storeToPut = storeManager.getStore(putRequest.getBlobId().getPartition());
      storeToPut.put(writeset);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                                             putRequest.getClientId(),
                                             ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                                             putRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on a put {} ", e);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                                             putRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  public void handleGetRequest(Request request) throws IOException, InterruptedException {
    GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      ServerErrorCode error = validateRequest(getRequest.getPartition(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating get request failed with error {}", error);
        GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                               getRequest.getClientId(),
                                               error);
        requestResponseChannel.sendResponse(response, request);
      }

      Store storeToGet = storeManager.getStore(getRequest.getPartition());
      StoreInfo info = storeToGet.get(getRequest.getBlobIds());
      Send blobsToSend = new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag());
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             info.getMessageReadSetInfo(),
                                             blobsToSend,
                                             ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a get with error code {} and exception {}", e.getErrorCode(), e);
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (MessageFormatException e) {
      logger.error("Message format exception on a get with error code {} and exception {}", e.getErrorCode(), e);
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             ErrorMapping.getMessageFormatErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on a get {}", e);
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  public void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    DeleteRequest deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      ServerErrorCode error = validateRequest(deleteRequest.getBlobId().getPartition(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating delete request failed with error {}", error);
        DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                     deleteRequest.getClientId(),
                                                     error);
        requestResponseChannel.sendResponse(response, request);
      }
      MessageFormatInputStream stream = new DeleteMessageFormatInputStream(deleteRequest.getBlobId());
      MessageInfo info = new MessageInfo(deleteRequest.getBlobId(), stream.getSize());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      Store storeToDelete = storeManager.getStore(deleteRequest.getBlobId().getPartition());
      storeToDelete.delete(writeset);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on delete {}", e);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  public void handleTTLRequest(Request request) throws IOException, InterruptedException {
    TTLRequest ttlRequest = TTLRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      ServerErrorCode error = validateRequest(ttlRequest.getBlobId().getPartition(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating ttl request failed with error {}", error);
        TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                               ttlRequest.getClientId(),
                                               error);
        requestResponseChannel.sendResponse(response, request);
      }

      MessageFormatInputStream stream = new TTLMessageFormatInputStream(ttlRequest.getBlobId(), ttlRequest.getNewTTL());
      MessageInfo info = new MessageInfo(ttlRequest.getBlobId(), stream.getSize(), ttlRequest.getNewTTL());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      Store storeToUpdateTTL = storeManager.getStore(ttlRequest.getBlobId().getPartition());
      storeToUpdateTTL.updateTTL(writeset);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                             ttlRequest.getClientId(),
                                             ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                             ttlRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on ttl {}", e);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                             ttlRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  private ServerErrorCode validateRequest(PartitionId partition, boolean checkPartitionState) {
    // 1. check if partition exist on this node
    if (storeManager.getStore(partition) == null)
      return ServerErrorCode.Partition_Unknown;
    // 2. ensure the disk for the partition/replica is available
    List<ReplicaId> replicaIds = partition.getReplicaIds();
    for (ReplicaId replica : replicaIds)
      if (replica.getDataNodeId().getHostname() == currentNode.getHostname() &&
          replica.getDataNodeId().getPort() == currentNode.getPort()) {
        if (replica.getDiskId().getState() == HardwareState.UNAVAILABLE) {
          return ServerErrorCode.Disk_Unavailable;
        }
      }
    // 3. ensure if the partition can be written to
    if (checkPartitionState && partition.getPartitionState() == PartitionState.READ_ONLY)
      return ServerErrorCode.Partition_ReadOnly;
    return ServerErrorCode.No_Error;
  }
}