package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatMetrics;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.messageformat.TTLMessageFormatInputStream;
import com.github.ambry.network.NetworkRequestMetrics;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.Send;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.ReplicaMetadataRequest;
import com.github.ambry.shared.ReplicaMetadataResponse;
import com.github.ambry.shared.RequestResponseType;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.shared.TTLRequest;
import com.github.ambry.shared.TTLResponse;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreManager;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
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
  private final FindTokenFactory findTokenFactory;

  public AmbryRequests(StoreManager storeManager,
                         RequestResponseChannel requestResponseChannel,
                       ClusterMap clusterMap,
                       DataNodeId nodeId,
                       MetricRegistry registry,
                       FindTokenFactory findTokenFactory) {
    this.storeManager = storeManager;
    this.requestResponseChannel = requestResponseChannel;
    this.clusterMap = clusterMap;
    this.currentNode = nodeId;
    this.metrics = new ServerMetrics(registry);
    this.messageFormatMetrics = new MessageFormatMetrics(registry);
    this.findTokenFactory = findTokenFactory;
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
        case ReplicaMetadataRequest:
          handleReplicaMetadataRequest(request);
          break;
        default: throw new UnsupportedOperationException("Request type not supported");
      }
    }
    catch (Exception e) {
      logger.error("Error while handling request" + request + "Closing connection", e);
      requestResponseChannel.closeConnection(request);
    }
  }

  public void handlePutRequest(Request request) throws IOException, InterruptedException {
    PutRequest putRequest = PutRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.putBlobRequestQueueTimeInMs.update(requestQueueTime);
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
                                           Utils.addSecondsToEpochTime(
                                                   putRequest.getBlobProperties().getCreationTimeInMs(),
                                                   putRequest.getBlobProperties().getTimeToLiveInSeconds()));
        ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
        infoList.add(info);
        MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
        Store storeToPut = storeManager.getStore(putRequest.getBlobId().getPartition());
        storeToPut.put(writeset);
        response = new PutResponse(putRequest.getCorrelationId(),
                                   putRequest.getClientId(),
                                   ServerErrorCode.No_Error);
        metrics.blobSizeInBytes.update(putRequest.getBlobProperties().getBlobSize());
        metrics.blobUserMetadataSizeInBytes.update(putRequest.getUsermetadata().limit());
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
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      metrics.putBlobProcessingTimeInMs.update(processingTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new NetworkRequestMetrics(
                                                new HistogramMeasurement(metrics.putBlobResponseQueueTimeInMs),
                                                new HistogramMeasurement(metrics.putBlobSendTimeInMs),
                                                new HistogramMeasurement(metrics.putBlobTotalTimeInMs),
                                                totalTimeSpent));
  }

  public void handleGetRequest(Request request) throws IOException, InterruptedException {
    GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    HistogramMeasurement responseQueueTimeMeasurement = null;
    HistogramMeasurement responseSendTimeMeasurement = null;
    HistogramMeasurement responseTotalTimeMeasurement = null;
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob) {
      metrics.getBlobRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobRequestRate.mark();
      responseQueueTimeMeasurement = new HistogramMeasurement(metrics.getBlobResponseQueueTimeInMs);
      responseSendTimeMeasurement = new HistogramMeasurement(metrics.getBlobSendTimeInMs);
      responseTotalTimeMeasurement = new HistogramMeasurement(metrics.getBlobTotalTimeInMs);
    }
    else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties) {
      metrics.getBlobPropertiesRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobPropertiesRequestRate.mark();
      responseQueueTimeMeasurement = new HistogramMeasurement(metrics.getBlobPropertiesResponseQueueTimeInMs);
      responseSendTimeMeasurement = new HistogramMeasurement(metrics.getBlobPropertiesSendTimeInMs);
      responseTotalTimeMeasurement = new HistogramMeasurement(metrics.getBlobPropertiesTotalTimeInMs);
    }
    else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata) {
      metrics.getBlobUserMetadataRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobUserMetadataRequestRate.mark();
      responseQueueTimeMeasurement = new HistogramMeasurement(metrics.getBlobUserMetadataResponseQueueTimeInMs);
      responseSendTimeMeasurement = new HistogramMeasurement(metrics.getBlobUserMetadataSendTimeInMs);
      responseTotalTimeMeasurement = new HistogramMeasurement(metrics.getBlobUserMetadataTotalTimeInMs);
    }
    else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.All) {
      metrics.getBlobAllRequestQueueTimeInMs.update(requestQueueTime);
      metrics.getBlobAllRequestRate.mark();
      responseQueueTimeMeasurement = new HistogramMeasurement(metrics.getBlobAllResponseQueueTimeInMs);
      responseSendTimeMeasurement = new HistogramMeasurement(metrics.getBlobAllSendTimeInMs);
      responseTotalTimeMeasurement = new HistogramMeasurement(metrics.getBlobAllTotalTimeInMs);
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
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      if (getRequest.getMessageFormatFlag() == MessageFormatFlags.Blob)
        metrics.getBlobProcessingTimeInMs.update(processingTime);
      else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobProperties)
        metrics.getBlobPropertiesProcessingTimeInMs.update(processingTime);
      else if (getRequest.getMessageFormatFlag() == MessageFormatFlags.BlobUserMetadata)
        metrics.getBlobUserMetadataProcessingTimeInMs.update(processingTime);
    }
    requestResponseChannel.sendResponse(response, request, new NetworkRequestMetrics(responseQueueTimeMeasurement,
                                                                                     responseSendTimeMeasurement,
                                                                                     responseTotalTimeMeasurement,
                                                                                     totalTimeSpent));
  }

  public void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    DeleteRequest deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.deleteBlobRequestQueueTimeInMs.update(requestQueueTime);
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
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      totalTimeSpent += processingTime;
      metrics.deleteBlobProcessingTimeInMs.update(processingTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new NetworkRequestMetrics(
                                                new HistogramMeasurement(metrics.deleteBlobResponseQueueTimeInMs),
                                                new HistogramMeasurement(metrics.deleteBlobSendTimeInMs),
                                                new HistogramMeasurement(metrics.deleteBlobTotalTimeInMs),
                                                totalTimeSpent));
  }

  public void handleTTLRequest(Request request) throws IOException, InterruptedException {
    TTLRequest ttlRequest = TTLRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.ttlBlobRequestQueueTimeInMs.update(requestQueueTime);
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
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      startTime += processingTime;
      metrics.ttlBlobProcessingTimeInMs.update(processingTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new NetworkRequestMetrics(
                                                new HistogramMeasurement(metrics.ttlBlobResponseQueueTimeInMs),
                                                new HistogramMeasurement(metrics.ttlBlobSendTimeInMs),
                                                new HistogramMeasurement(metrics.ttlBlobTotalTimeInMs),
                                                totalTimeSpent));
  }

  public void handleReplicaMetadataRequest(Request request) throws IOException, InterruptedException {
    ReplicaMetadataRequest replicaMetadataRequest =
            ReplicaMetadataRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap, findTokenFactory);
    long requestQueueTime = SystemTime.getInstance().milliseconds() - request.getStartTimeInMs();
    long totalTimeSpent = requestQueueTime;
    metrics.replicaMetadataRequestQueueTimeInMs.update(requestQueueTime);
    metrics.replicaMetadataRequestRate.mark();
    long startTime = SystemTime.getInstance().milliseconds();
    ReplicaMetadataResponse response = null;
    try {
      ServerErrorCode error = validateRequest(replicaMetadataRequest.getPartitionId(), false);
      if (error != ServerErrorCode.No_Error) {
        logger.error("Validating replica metadata request failed with error {}", error);
        response = new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(),
                                               replicaMetadataRequest.getClientId(),
                                               error);
      }
      Store store = storeManager.getStore(replicaMetadataRequest.getPartitionId());
      FindInfo findInfo = store.findEntriesSince(replicaMetadataRequest.getToken(),
                                                 replicaMetadataRequest.getMaxTotalSizeOfEntriesInBytes());
      response = new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(),
                                             replicaMetadataRequest.getClientId(),
                                             ServerErrorCode.No_Error,
                                             findInfo.getFindToken(),
                                             findInfo.getMessageEntries());
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      if (e.getErrorCode() == StoreErrorCodes.IOError)
        metrics.storeIOError.inc();
      else
        metrics.unExpectedStoreFindEntriesError.inc();
      response = new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(),
                                             replicaMetadataRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
    }
    catch (Exception e) {
      logger.error("Unknown exception on replica metadata request ", e);
      response = new ReplicaMetadataResponse(replicaMetadataRequest.getCorrelationId(),
                                             replicaMetadataRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
    }
    finally {
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      startTime += processingTime;
      metrics.replicaMetadataRequestProcessingTimeInMs.update(processingTime);
    }
    requestResponseChannel.sendResponse(response,
                                        request,
                                        new NetworkRequestMetrics(
                                                new HistogramMeasurement(metrics.replicaMetadataResponseQueueTimeInMs),
                                                new HistogramMeasurement(metrics.replicaMetadataSendTimeInMs),
                                                new HistogramMeasurement(metrics.replicaMetadataTotalTimeInMs),
                                                totalTimeSpent));
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
