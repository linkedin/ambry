package com.github.ambry.router;

import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobProperties;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;


class PutChunk {

}

class MetadataPutChunk {

}

public class PutManager {
  private Thread ChunkFillerThread;
  private ConcurrentSkipListMap<Long, PutOperation> putOperations;
  private final long chunkSize;

  public PutManager(long chunkSize) {
    this.chunkSize = chunkSize;
  }

  public FutureResult<String> submitPutBlobOperation(long operationId, BlobProperties blobProperties,
      byte[] usermetadata, ReadableStreamChannel channel, Callback<String> callback) {
    FutureResult<String> futureResult = new FutureResult<String>();
    PutOperation putOperation =
        new PutOperation(operationId, blobProperties, usermetadata, channel, futureResult, callback);
    putOperations.put(operationId, putOperation);
    return futureResult;
  }

  List<String> getIdsToDelete() {
    // @todo save and return ids of failed puts.
    return null;
  }

  class PutOperation {
    private long operationId;
    private BlobId blobId;
    private final BlobProperties blobProperties;
    private final byte[] userMetadata;
    private final ReadableStreamChannel channel;
    private final FutureResult<String> futureResult;
    private final Callback<String> callback;
    private MetadataPutChunk metadataPutChunk;
    private PutChunk[] putChunks;
    private final long numChunks;

    PutOperation(long operationId, BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
        FutureResult<String> futureResult, Callback<String> callback) {
      this.operationId = operationId;
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.channel = channel;
      this.futureResult = futureResult;
      this.callback = callback;
      this.numChunks = blobProperties.getBlobSize() == 0 ? 1 : (blobProperties.getBlobSize() - 1) / chunkSize + 1;
    }
  }
}
