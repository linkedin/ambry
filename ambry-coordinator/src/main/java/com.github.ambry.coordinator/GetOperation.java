package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.RequestOrResponse;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.utils.BlockingChannelPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

/**
 * Performs a get operation by sending and receiving get requests until operation is complete or has failed.
 */
public abstract class GetOperation extends Operation {
  protected ClusterMap clusterMap;
  private MessageFormatFlags flags;
  private GetResponsePolicy getResponsePolicy;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public GetOperation(String datacenterName, BlockingChannelPool connectionPool, ExecutorService requesterPool,
                      OperationContext oc, BlobId blobId, long operationTimeoutMs,
                      ClusterMap clusterMap, MessageFormatFlags flags) throws CoordinatorException {
    super(datacenterName, connectionPool, requesterPool, oc, blobId, operationTimeoutMs,
          new GetPolicy(datacenterName, blobId.getPartition()));
    this.clusterMap = clusterMap;
    this.flags = flags;
    this.getResponsePolicy = new GetThresholdResponsePolicy(blobId.getPartition());
  }

  protected abstract class GetOperationRequest extends OperationRequest {
    protected GetOperationRequest(ReplicaId replicaId, RequestOrResponse request) {
      super(replicaId, request);
    }

    protected Response getResponse(DataInputStream dataInputStream) throws IOException {
      return GetResponse.readFrom(dataInputStream, clusterMap);
    }

    @Override
    protected void deserializeResponsePayload(Response response) throws CoordinatorException, IOException, MessageFormatException {
      GetResponse getResponse = (GetResponse)response;
      if (response.getError() == ServerErrorCode.No_Error) {
        if (getResponse.getMessageInfoList().size() != 1) {
          throw new CoordinatorException("MessageInfoList indicates incorrect payload size. Should be 1: " +
                                         getResponse.getMessageInfoList().size(),
                                         CoordinatorError.UnexpectedInternalError);
        }
        deserializeBody(getResponse.getInputStream());
      }
    }

    protected abstract void deserializeBody(InputStream inputStream) throws IOException, MessageFormatException;
  }

  protected GetRequest makeGetRequest() {
    ArrayList<BlobId> blobIds = new ArrayList<BlobId>(1);
    blobIds.add(blobId);
    return new GetRequest(oc.getCorrelationId(), oc.getClientId(), flags, blobId.getPartition(), blobIds);
  }

  @Override
  protected boolean processResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) throws
          CoordinatorException {
    switch (serverErrorCode) {
      case No_Error:
        return true;
      case IO_Error:
      case Data_Corrupt:
        return false;
      case Blob_Not_Found:
        getResponsePolicy.blobNotFound(replicaId);
        return false;
      case Blob_Deleted:
        getResponsePolicy.blobDeleted(replicaId);
        return false;
      case Blob_Expired:
        getResponsePolicy.blobExpired(replicaId);
        return false;
      default:
        logger.error("{} GetResponse for BlobId {} received from ReplicaId {} had unexpected error code {}",
                     oc, blobId, replicaId, serverErrorCode);
        throw new CoordinatorException("Unexpected server error code in GetResponse.",
                                       CoordinatorError.UnexpectedInternalError);
    }
  }
}
