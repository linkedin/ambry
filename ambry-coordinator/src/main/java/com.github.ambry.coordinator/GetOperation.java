package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PartitionRequestInfo;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;


/**
 * Performs a get operation by sending and receiving get requests until operation is complete or has failed.
 */
public abstract class GetOperation extends Operation {
  ClusterMap clusterMap;
  private MessageFormatFlags flags;

  private int blobNotFoundCount;
  private int blobDeletedCount;
  private int blobExpiredCount;

  // Number of replicas in the partition. This is used to set threshold to determine blob not found (all replicas
  // must reply). Also used to modify blob deleted and blob expired thresholds for partitions with a small number of
  // replicas.
  private final int replicaIdCount;
  // Minimum number of Blob_Deleted responses from servers necessary before returning Blob_Deleted to caller.
  private static final int Blob_Deleted_Count_Threshold = 1;
  // Minimum number of Blob_Expired responses from servers necessary before returning Blob_Expired to caller.
  private static final int Blob_Expired_Count_Threshold = 2;

  private Logger logger = LoggerFactory.getLogger(getClass());
  protected static HashMap<CoordinatorError, Integer> precedenceLevels = new HashMap<CoordinatorError, Integer>();

  public GetOperation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext oc, BlobId blobId, long operationTimeoutMs, ClusterMap clusterMap, MessageFormatFlags flags)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs,
        new GetPolicy(datacenterName, blobId.getPartition()));
    this.clusterMap = clusterMap;
    this.flags = flags;

    this.replicaIdCount = blobId.getPartition().getReplicaIds().size();
    this.blobNotFoundCount = 0;
    this.blobDeletedCount = 0;
    this.blobExpiredCount = 0;
  }

  static {
    precedenceLevels.put(CoordinatorError.BlobDeleted, 1);
    precedenceLevels.put(CoordinatorError.BlobExpired, 2);
    precedenceLevels.put(CoordinatorError.AmbryUnavailable, 3);
    precedenceLevels.put(CoordinatorError.UnexpectedInternalError, 4);
    precedenceLevels.put(CoordinatorError.BlobDoesNotExist, 5);
  }


  GetRequest makeGetRequest() {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>(1);
    blobIds.add(blobId);
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobId.getPartition(), blobIds);
    partitionRequestInfoList.add(partitionRequestInfo);
    return new GetRequest(context.getCorrelationId(), context.getClientId(), flags, partitionRequestInfoList);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode)
      throws CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
        return true;
      case IO_Error:
        logger.trace("Server returned IO error for GetOperation");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        return false;
      case Data_Corrupt:
        logger.trace("Server returned Data Corrupt error for GetOperation");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        return false;
      case Blob_Not_Found:
        blobNotFoundCount++;
        if (blobNotFoundCount == replicaIdCount) {
          String message =
              "GetOperation : Blob not found : blobNotFoundCount == replicaIdCount == " + blobNotFoundCount + ".";
          logger.trace(message);
          throw new CoordinatorException(message, CoordinatorError.BlobDoesNotExist);
        }
        setCurrentError(CoordinatorError.BlobDoesNotExist);
        return false;
      case Blob_Deleted:
        blobDeletedCount++;
        if (blobDeletedCount >= min(Blob_Deleted_Count_Threshold, replicaIdCount)) {
          String message =
              "GetOperation : Blob deleted : blobDeletedCount == " + blobDeletedCount + " >= min(deleteThreshold == "
                  + Blob_Deleted_Count_Threshold + ", replicaIdCount == " + replicaIdCount + ").";
          logger.trace(message);
          throw new CoordinatorException(message, CoordinatorError.BlobDeleted);
        }
        setCurrentError(CoordinatorError.BlobDeleted);
        return false;
      case Blob_Expired:
        blobExpiredCount++;
        if (blobExpiredCount >= min(Blob_Expired_Count_Threshold, replicaIdCount)) {
          String message =
              "GetOperation : Blob expired : blobExpiredCount == " + blobExpiredCount + " >= min(expiredThreshold == "
                  + Blob_Expired_Count_Threshold + ", replicaIdCount == " + replicaIdCount + ").";
          logger.trace(message);
          throw new CoordinatorException(message, CoordinatorError.BlobExpired);
        }
        setCurrentError(CoordinatorError.BlobExpired);
        return false;
      case Disk_Unavailable:
        logger.trace("Server returned Disk Unavailable error for GetOperation");
        setCurrentError(CoordinatorError.AmbryUnavailable);
        return false;
      case Partition_Unknown:
        logger.trace("Server returned Partition Unknown error for GetOperation");
        setCurrentError(CoordinatorError.BlobDoesNotExist);
        return false;
      default:
        CoordinatorException e = new CoordinatorException("Server returned unexpected error for GetOperation.",
            CoordinatorError.UnexpectedInternalError);
        logger
            .error("{} GetResponse for BlobId {} received from ReplicaId {} had unexpected error code {}: {}", context,
                blobId, replicaId, serverErrorCode, e);
        throw e;
    }
  }

  @Override
  public Integer getPrecedenceLevel(CoordinatorError coordinatorError) {
    return precedenceLevels.get(coordinatorError);
  }
}

abstract class GetOperationRequest extends OperationRequest {
  private final ClusterMap clusterMap;
  private Logger logger = LoggerFactory.getLogger(getClass());

  protected GetOperationRequest(ConnectionPool connectionPool, BlockingQueue<OperationResponse> responseQueue,
      OperationContext context, BlobId blobId, ReplicaId replicaId, RequestOrResponse request, ClusterMap clusterMap) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request);
    this.clusterMap = clusterMap;
  }

  @Override
  protected Response getResponse(DataInputStream dataInputStream)
      throws IOException {
    return GetResponse.readFrom(dataInputStream, clusterMap);
  }

  @Override
  protected void deserializeResponsePayload(Response response)
      throws IOException, MessageFormatException {
    GetResponse getResponse = (GetResponse) response;
    if (response.getError() == ServerErrorCode.No_Error) {
      if (getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().size() != 1) {
        String message = "MessageInfoList indicates incorrect payload size. Should be 1: " + getResponse
            .getPartitionResponseInfoList().get(0).getMessageInfoList().size();
        logger.error(message);
        throw new MessageFormatException(message, MessageFormatErrorCodes.Data_Corrupt);
      }
      deserializeBody(getResponse.getInputStream());
    }
  }

  protected abstract void deserializeBody(InputStream inputStream)
      throws IOException, MessageFormatException;
}


