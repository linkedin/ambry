package com.github.ambry.server;

import java.io.DataInputStream;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.*;
import com.github.ambry.messageformat.*;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.shared.*;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.store.*;
import com.github.ambry.utils.SystemTime;
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
  private final ServerMetrics metrics;
  private final MessageFormatMetrics messageFormatMetrics;

  public AmbryRequests(StoreManager storeManager,
                       RequestResponseChannel requestResponseChannel,
                       ClusterMap clusterMap,
                       DataNodeId nodeId,
                       MetricRegistry registry) {
    this.storeManager = storeManager;
    this.requestResponseChannel = requestResponseChannel;
    this.clusterMap = clusterMap;
    this.currentNode = nodeId;
    this.metrics = new ServerMetrics(registry);
    this.messageFormatMetrics = new MessageFormatMetrics(registry);
  }

  public void handleRequests(Request request) throws InterruptedException {
    try {
      logger.trace("{}", request);
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
    metrics.putBlobRequestQueueTime.update(SystemTime.getInstance().milliseconds() - request.getStartTimeInMs());
    metrics.putBlobRequestRate.mark();
    long startTime = SystemTime.getInstance().milliseconds();
    PutResponse response = null;
    try {
      ServerErrorCode error = validateRequest(putRequest.getBlobId().getPartition(), true);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating put request failed with error {}", error);
        response = new PutResponse(putRequest.getCorrelationId(),
                                   putRequest.getClientId(),
                                   error);
      }
      else {
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
        response = new PutResponse(putRequest.getCorrelationId(),
                                   putRequest.getClientId(),
                                   ServerErrorCode.No_Error);
      }
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      if (e.getErrorCode() == StoreErrorCodes.Already_Exist)
        metrics.idAlreadyExistError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.IOError)
        metrics.storeIOError.inc();
      else
        metrics.unExpectedStorePutError.inc();
      response = new PutResponse(putRequest.getCorrelationId(),
                                 putRequest.getClientId(),
                                 ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    }
    catch (Exception e) {
      logger.error("Unknown exception on a put {} ", e);
      response = new PutResponse(putRequest.getCorrelationId(),
                                 putRequest.getClientId(),
                                 ServerErrorCode.Unknown_Error);
    }
    finally {
      metrics.putBlobProcessingTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new HistogramMeasurement(metrics.putBlobResponseQueueTime),
                                        new HistogramMeasurement(metrics.putBlobSendTime));
  }

  public void handleGetRequest(Request request) throws IOException, InterruptedException {
    GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    HistogramMeasurement responseQueueMeasurement = null;
    HistogramMeasurement responseSendMeasurement = null;
    if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
      metrics.getBlobRequestQueueTime.update(SystemTime.getInstance().milliseconds() - request.getStartTimeInMs());
      metrics.getBlobRequestRate.mark();
      responseQueueMeasurement = new HistogramMeasurement(metrics.getBlobResponseQueueTime);
      responseSendMeasurement = new HistogramMeasurement(metrics.getBlobSendTime);
    }
    else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
      metrics.getBlobPropertiesRequestQueueTime.update(SystemTime.getInstance().milliseconds() - request.getStartTimeInMs());
      metrics.getBlobPropertiesRequestRate.mark();
      responseQueueMeasurement = new HistogramMeasurement(metrics.getBlobPropertiesResponseQueueTime);
      responseSendMeasurement = new HistogramMeasurement(metrics.getBlobPropertiesSendTime);
    }
    else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
      metrics.getBlobUserMetadataRequestQueueTime.update(SystemTime.getInstance().milliseconds() - request.getStartTimeInMs());
      metrics.getBlobUserMetadataRequestRate.mark();
      responseQueueMeasurement = new HistogramMeasurement(metrics.getBlobUserMetadataResponseQueueTime);
      responseSendMeasurement = new HistogramMeasurement(metrics.getBlobUserMetadataSendTime);
    }
    long startTime = SystemTime.getInstance().milliseconds();
    GetResponse response = null;
    try {
      ServerErrorCode error = validateRequest(getRequest.getPartition(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating get request failed with error {}", error);
        response = new GetResponse(getRequest.getCorrelationId(),
                                   getRequest.getClientId(),
                                   error);
      }
      else {
        Store storeToGet = storeManager.getStore(getRequest.getPartition());
        StoreInfo info = storeToGet.get(getRequest.getBlobIds());
        Send blobsToSend = new MessageFormatSend(info.getMessageReadSet(),
                                                 getRequest.getMessageFormatFlag(),
                                                 messageFormatMetrics);
        response = new GetResponse(getRequest.getCorrelationId(),
                                               getRequest.getClientId(),
                                               info.getMessageReadSetInfo(),
                                               blobsToSend,
                                               ServerErrorCode.No_Error);
      }
    }
    catch (StoreException e) {
      logger.error("Store exception on a get with error code {} and exception {}", e.getErrorCode(), e);
      if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found)
        metrics.idNotFoundError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired)
        metrics.ttlExpiredError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted)
        metrics.idDeletedError.inc();
      else
        metrics.unExpectedStoreGetError.inc();
      response = new GetResponse(getRequest.getCorrelationId(),
                                 getRequest.getClientId(),
                                 ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    }
    catch (MessageFormatException e) {
      logger.error("Message format exception on a get with error code {} and exception {}", e.getErrorCode(), e);
      if (e.getErrorCode() == MessageFormatErrorCodes.Data_Corrupt)
        metrics.dataCorruptError.inc();
      else if (e.getErrorCode() == MessageFormatErrorCodes.Unknown_Format_Version)
        metrics.unknownFormatError.inc();
      response = new GetResponse(getRequest.getCorrelationId(),
                                 getRequest.getClientId(),
                                 ErrorMapping.getMessageFormatErrorMapping(e.getErrorCode()));
    }
    catch (Exception e) {
      logger.error("Unknown exception on a get {}", e);
      response = new GetResponse(getRequest.getCorrelationId(),
                                 getRequest.getClientId(),
                                 ServerErrorCode.Unknown_Error);
    }
    finally {
      if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob)
        metrics.getBlobProcessingTime.update(SystemTime.getInstance().milliseconds() - startTime);
      else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties)
        metrics.getBlobPropertiesProcessingTime.update(SystemTime.getInstance().milliseconds() - startTime);
      else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata)
        metrics.getBlobUserMetadataProcessingTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    requestResponseChannel.sendResponse(response, request, responseQueueMeasurement, responseSendMeasurement);
  }

  public void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    DeleteRequest deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    metrics.deleteBlobRequestQueueTime.update(SystemTime.getInstance().milliseconds() - request.getStartTimeInMs());
    metrics.deleteBlobRequestRate.mark();
    long startTime = SystemTime.getInstance().milliseconds();
    DeleteResponse response = null;
    try {
      ServerErrorCode error = validateRequest(deleteRequest.getBlobId().getPartition(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating delete request failed with error {}", error);
        response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                      deleteRequest.getClientId(),
                                      error);
      }
      else {
        MessageFormatInputStream stream = new DeleteMessageFormatInputStream(deleteRequest.getBlobId());
        MessageInfo info = new MessageInfo(deleteRequest.getBlobId(), stream.getSize());
        ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
        infoList.add(info);
        MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
        Store storeToDelete = storeManager.getStore(deleteRequest.getBlobId().getPartition());
        storeToDelete.delete(writeset);
        response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                      deleteRequest.getClientId(),
                                      ServerErrorCode.No_Error);
      }
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found)
        metrics.idNotFoundError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired)
        metrics.ttlExpiredError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted)
        metrics.idDeletedError.inc();
      else
        metrics.unExpectedStoreDeleteError.inc();
      response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                    deleteRequest.getClientId(),
                                    ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    }
    catch (Exception e) {
      logger.error("Unknown exception on delete {}", e);
      response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                    deleteRequest.getClientId(),
                                    ServerErrorCode.Unknown_Error);
    }
    finally {
      metrics.deleteBlobProcessingTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new HistogramMeasurement(metrics.deleteBlobResponseQueueTime),
                                        new HistogramMeasurement(metrics.deleteBlobSendTime));
  }

  public void handleTTLRequest(Request request) throws IOException, InterruptedException {
    TTLRequest ttlRequest = TTLRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    metrics.ttlBlobRequestQueueTime.update(SystemTime.getInstance().milliseconds() - request.getStartTimeInMs());
    metrics.ttlBlobRequestRate.mark();
    long startTime = SystemTime.getInstance().milliseconds();
    TTLResponse response = null;
    try {
      ServerErrorCode error = validateRequest(ttlRequest.getBlobId().getPartition(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating ttl request failed with error {}", error);
        response = new TTLResponse(ttlRequest.getCorrelationId(),
                                   ttlRequest.getClientId(),
                                   error);
      }
      else {
        MessageFormatInputStream stream = new TTLMessageFormatInputStream(ttlRequest.getBlobId(), ttlRequest.getNewTTL());
        MessageInfo info = new MessageInfo(ttlRequest.getBlobId(), stream.getSize(), ttlRequest.getNewTTL());
        ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
        infoList.add(info);
        MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
        Store storeToUpdateTTL = storeManager.getStore(ttlRequest.getBlobId().getPartition());
        storeToUpdateTTL.updateTTL(writeset);
        response = new TTLResponse(ttlRequest.getCorrelationId(),
                                   ttlRequest.getClientId(),
                                   ServerErrorCode.No_Error);
      }
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      if (e.getErrorCode() == StoreErrorCodes.ID_Not_Found)
        metrics.idNotFoundError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.TTL_Expired)
        metrics.ttlExpiredError.inc();
      else if (e.getErrorCode() == StoreErrorCodes.ID_Deleted)
        metrics.idDeletedError.inc();
      else
        metrics.unExpectedStoreTTLError.inc();
      response = new TTLResponse(ttlRequest.getCorrelationId(),
                                 ttlRequest.getClientId(),
                                 ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    }
    catch (Exception e) {
      logger.error("Unknown exception on ttl {}", e);
      response = new TTLResponse(ttlRequest.getCorrelationId(),
                                 ttlRequest.getClientId(),
                                 ServerErrorCode.Unknown_Error);
    }
    finally {
      metrics.ttlBlobProcessingTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new HistogramMeasurement(metrics.ttlBlobResponseQueueTime),
                                        new HistogramMeasurement(metrics.ttlBlobSendTime));
  }

  private ServerErrorCode validateRequest(PartitionId partition, boolean checkPartitionState) {
    // 1. check if partition exist on this node
    if (storeManager.getStore(partition) == null) {
      metrics.partitionUnknownError.inc();
      return ServerErrorCode.Partition_Unknown;
    }
    // 2. ensure the disk for the partition/replica is available
    List<ReplicaId> replicaIds = partition.getReplicaIds();
    for (ReplicaId replica : replicaIds)
      if (replica.getDataNodeId().getHostname() == currentNode.getHostname() &&
          replica.getDataNodeId().getPort() == currentNode.getPort()) {
        if (replica.getDiskId().getState() == HardwareState.UNAVAILABLE) {
          metrics.diskUnavailableError.inc();
          return ServerErrorCode.Disk_Unavailable;
        }
      }
    // 3. ensure if the partition can be written to
    if (checkPartitionState && partition.getPartitionState() == PartitionState.READ_ONLY) {
      metrics.partitionReadOnlyError.inc();
      return ServerErrorCode.Partition_ReadOnly;
    }
    return ServerErrorCode.No_Error;
  }
}
