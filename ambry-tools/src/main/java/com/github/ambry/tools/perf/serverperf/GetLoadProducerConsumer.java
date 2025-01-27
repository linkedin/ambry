package com.github.ambry.tools.perf.serverperf;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.tools.perf.serverperf.ServerPerformance.ServerPerformanceConfig;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetLoadProducerConsumer implements LoadProducerConsumer {

  private final ServerPerfNetworkQueue networkQueue;
  private final ServerPerformanceConfig config;
  private final ClusterMap clusterMap;

  private final AtomicInteger correlationId = new AtomicInteger();
  private static final String CLIENT_ID = "ServerReadPerformance";
  private static final Logger logger = LoggerFactory.getLogger(GetLoadProducerConsumer.class);

  public GetLoadProducerConsumer(ServerPerfNetworkQueue networkQueue, ServerPerformanceConfig config,
      ClusterMap clusterMap) {
    this.networkQueue = networkQueue;
    this.config = config;
    this.clusterMap = clusterMap;
  }

  @Override
  public void produce() throws Exception {
    final BufferedReader br = new BufferedReader(new FileReader(config.serverPerformanceBlobIdFilePath));
    String line;
    boolean isShutDown = false;

    while ((line = br.readLine()) != null) {
      String[] id = line.split("\n");
      BlobId blobId = new BlobId(id[0], clusterMap);

      PartitionRequestInfo partitionRequestInfo =
          new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
      GetRequest getRequest = new GetRequest(correlationId.incrementAndGet(), CLIENT_ID, MessageFormatFlags.Blob,
          Collections.singletonList(partitionRequestInfo), GetOption.Include_All);
      DataNodeId dataNodeId = clusterMap.getDataNodeId(config.serverPerformanceHostname, config.serverPerformancePort);
      ReplicaId replicaId =
          getReplicaFromNode(dataNodeId, getRequest.getPartitionInfoList().get(0).getPartition(), clusterMap);
      String hostname = dataNodeId.getHostname();
      Port port = dataNodeId.getPortToConnectTo();
      RequestInfo requestInfo = new RequestInfo(hostname, port, getRequest, replicaId, null);
      logger.info("submitting the blob id {} to network queue correlation id {}", blobId,
          requestInfo.getRequest().getCorrelationId());
      try {
        networkQueue.submit(requestInfo);
      } catch (ShutDownException e) {
        isShutDown = true;
        break;
      }
    }
    br.close();

    if (isShutDown) {
      throw new ShutDownException();
    }
  }

  private ReplicaId getReplicaFromNode(DataNodeId dataNodeId, PartitionId partitionId, ClusterMap clusterMap) {
    ReplicaId replicaToReturn = null;
    if (partitionId != null) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        if (replicaId.getDataNodeId().getHostname().equals(dataNodeId.getHostname())) {
          replicaToReturn = replicaId;
          break;
        }
      }
    } else {
      // pick any replica on this node
      replicaToReturn = clusterMap.getReplicaIds(dataNodeId).get(0);
    }
    return replicaToReturn;
  }

  @Override
  public void consume() throws Exception {
    try {
      networkQueue.poll(this::processGetResponse);
    } catch (ShutDownException e) {
      logger.info("Network queue is shutdown");
      throw e;
    } catch (Exception e) {
      logger.error("error in load consumer thread", e);
    }
    logger.info("Load consumer thread is finished");
  }

  void processGetResponse(ResponseInfo responseInfo) {
    try {
      if (responseInfo.getError() == NetworkClientErrorCode.TimeoutError) {
        logger.info("Timeout error for correlation id {}",
            responseInfo.getRequestInfo().getRequest().getCorrelationId());
        return;
      }
      InputStream serverResponseStream = new NettyByteBufDataInputStream(responseInfo.content());
      GetResponse getResponse = GetResponse.readFrom(new DataInputStream(serverResponseStream), clusterMap);
      ServerErrorCode partitionErrorCode = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
      ServerErrorCode errorCode =
          partitionErrorCode == ServerErrorCode.No_Error ? getResponse.getError() : partitionErrorCode;
      InputStream stream = errorCode == ServerErrorCode.No_Error ? getResponse.getInputStream() : null;
      BlobData blobData = stream != null ? MessageFormatRecord.deserializeBlob(stream) : null;
      long blobDataSize = blobData != null ? blobData.getSize() : 0;
      responseInfo.release();
      getResponse.release();
      logger.info("blob id {} blob size {}  correlation id {}",
          getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getStoreKey(), blobDataSize,
          responseInfo.getRequestInfo().getRequest().getCorrelationId());
    } catch (Exception e) {
      logger.error("error in processing get response", e);
    }
  }
}
