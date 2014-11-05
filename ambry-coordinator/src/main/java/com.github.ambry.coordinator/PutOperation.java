package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.utils.ByteBufferInputStream;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;


/**
 * Performs a put operation by sending and receiving put requests until operation is complete or has failed.
 * <p/>
 * Note that a put operation that partially completes may eventually "complete" in the background. However, the BlobId
 * is never returned to the caller and so the blob will never be retrieved or deleted. This could "leak" space and, in
 * the future, lead to customers being "billed" for space consumption of a blob they don't know they stored.
 */
final public class PutOperation extends Operation {
  private final BlobProperties blobProperties;
  private final ByteBuffer userMetadata;
  private final ByteBufferInputStream materializedBlobStream;

  private Logger logger = LoggerFactory.getLogger(getClass());
  private static HashMap<CoordinatorError, Integer> precedenceLevels = new HashMap<CoordinatorError, Integer>();

  public PutOperation(String datacenterName, ConnectionPool connectionPool, ExecutorService requesterPool,
      OperationContext oc, BlobId blobId, long operationTimeoutMs, BlobProperties blobProperties,
      ByteBuffer userMetadata, InputStream blobStream)
      throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs,
        new PutPolicy(datacenterName, blobId.getPartition(), oc.isCrossDCProxyCallEnabled()));
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;

    try {
      this.materializedBlobStream = new ByteBufferInputStream(blobStream, (int) blobProperties.getBlobSize());
    } catch (IOException e) {
      CoordinatorException ce = new CoordinatorException("Error processing blob passed into PutOperation.", e,
          CoordinatorError.UnexpectedInternalError);
      logger.error("Could not materialize blob ", ce);
      throw ce;
    }
  }

  static {
    precedenceLevels.put(CoordinatorError.UnexpectedInternalError, 1);
    precedenceLevels.put(CoordinatorError.AmbryUnavailable, 2);
  }

  @Override
  protected OperationRequest makeOperationRequest(ReplicaId replicaId) {
    PutRequest putRequest = new PutRequest(context.getCorrelationId(), context.getClientId(), blobId, blobProperties,
        userMetadata.duplicate(), materializedBlobStream.duplicate());
    return new PutOperationRequest(connectionPool, responseQueue, context, blobId, replicaId, putRequest);
  }

  @Override
  protected ServerErrorCode processResponseError(ReplicaId replicaId, Response response)
      throws CoordinatorException {
    switch (response.getError()) {
      case No_Error:
        break;
      case IO_Error:
        logger.trace(context + " Server returned IO error for PutOperation for ");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        break;
      case Partition_ReadOnly:
        logger.trace(context + " Server returned Partition ReadOnly error for PutOperation ");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        break;
      case Disk_Unavailable:
        logger.trace(context + " Server returned Disk Unavailable error for PutOperation ");
        setCurrentError(CoordinatorError.AmbryUnavailable);
        break;
      case Partition_Unknown:
        logger.trace(context + " Server returned Partition Unknown error for PutOperation ");
        setCurrentError(CoordinatorError.UnexpectedInternalError);
        break;
      case Blob_Already_Exists:
        CoordinatorException e =
            new CoordinatorException("BlobId already exists.", CoordinatorError.UnexpectedInternalError);
        logger.error(context + " Put issued to BlobId " + blobId + " that already exists on ReplicaId " + replicaId, e);
        throw e;
      /*
      case Replica_Not_Writable:
        logger.error("{} Put issued to BlobId {} on read only partition (ReplicaId {} responded with {}).",
                     context, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Attempt to write to read-only partition.",
        CoordinatorError.UnexpectedInternalError);
       */
      default:
        e = new CoordinatorException("Server returned unexpected error for PutOperation.",
            CoordinatorError.UnexpectedInternalError);
        logger.error(context + " PutResponse for BlobId " +
            blobId + " received from ReplicaId " +
            replicaId + " had unexpected error code " + response.getError(), e);
        throw e;
    }
    return response.getError();
  }

  @Override
  public Integer getPrecedenceLevel(CoordinatorError coordinatorError) {
    return precedenceLevels.get(coordinatorError);
  }
}

final class PutOperationRequest extends OperationRequest {
  private Logger logger = LoggerFactory.getLogger(getClass());
  protected PutOperationRequest(ConnectionPool connectionPool, BlockingQueue<OperationResponse> responseQueue,
      OperationContext context, BlobId blobId, ReplicaId replicaId, RequestOrResponse request) {
    super(connectionPool, responseQueue, context, blobId, replicaId, request);
    logger.trace("Created PutOperationRequest for " + replicaId);
  }

  @Override
  protected void markRequest()
      throws CoordinatorException {
    context.getCoordinatorMetrics().getRequestMetrics(replicaId.getDataNodeId()).putBlobRequestRate.mark();
  }

  @Override
  protected void updateRequest(long durationInMs)
      throws CoordinatorException {
    context.getCoordinatorMetrics().
        getRequestMetrics(replicaId.getDataNodeId()).putBlobRequestLatencyInMs.update(durationInMs);
  }

  @Override
  protected Response getResponse(DataInputStream dataInputStream)
      throws IOException {
    return PutResponse.readFrom(dataInputStream);
  }
}


