/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.Response;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.github.ambry.replication.ReplicationTest.*;
import static org.junit.Assert.*;


/**
 * Implementation of {@link ConnectionPool} that hands out {@link MockConnection}s.
 */
public class MockConnectionPool implements ConnectionPool {
  private final Map<DataNodeId, MockHost> hosts;
  private final ClusterMap clusterMap;
  private final int maxEntriesToReturn;

  public MockConnectionPool(Map<DataNodeId, MockHost> hosts, ClusterMap clusterMap, int maxEntriesToReturn) {
    this.hosts = hosts;
    this.clusterMap = clusterMap;
    this.maxEntriesToReturn = maxEntriesToReturn;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public ConnectedChannel checkOutConnection(String host, Port port, long timeout) {
    DataNodeId dataNodeId = clusterMap.getDataNodeId(host, port.getPort());
    MockHost hostObj = hosts.get(dataNodeId);
    return new MockConnection(hostObj, maxEntriesToReturn);
  }

  @Override
  public void checkInConnection(ConnectedChannel connectedChannel) {
  }

  @Override
  public void destroyConnection(ConnectedChannel connectedChannel) {
  }

  /**
   * Implementation of {@link ConnectedChannel} that fetches message infos or blobs based on the type of request.
   */
  public static class MockConnection implements ConnectedChannel {

    class MockSend implements Send {

      private final List<ByteBuffer> buffers;
      private final int size;
      private int index;

      MockSend(List<ByteBuffer> buffers) {
        this.buffers = buffers;
        index = 0;
        int runningSize = 0;
        for (ByteBuffer buffer : buffers) {
          runningSize += buffer.remaining();
        }
        size = runningSize;
      }

      @Override
      public long writeTo(WritableByteChannel channel) throws IOException {
        ByteBuffer bufferToWrite = buffers.get(index);
        index++;
        int savedPos = bufferToWrite.position();
        long written = channel.write(bufferToWrite);
        bufferToWrite.position(savedPos);
        return written;
      }

      @Override
      public boolean isSendComplete() {
        return buffers.size() == index;
      }

      @Override
      public long sizeInBytes() {
        return size;
      }
    }

    private final MockHost host;
    private final int maxSizeToReturn;

    private List<ByteBuffer> buffersToReturn;
    private Map<PartitionId, List<MessageInfo>> infosToReturn;
    private Map<PartitionId, List<MessageMetadata>> messageMetadatasToReturn;
    private Map<StoreKey, StoreKey> conversionMap;
    private ReplicaMetadataRequest metadataRequest;
    private GetRequest getRequest;

    MockConnection(MockHost host, int maxSizeToReturn) {
      this(host, maxSizeToReturn, null);
    }

    MockConnection(MockHost host, int maxSizeToReturn, Map<StoreKey, StoreKey> conversionMap) {
      this.host = host;
      this.maxSizeToReturn = maxSizeToReturn;
      this.conversionMap = conversionMap == null ? new HashMap<>() : conversionMap;
    }

    @Override
    public void connect() throws IOException {

    }

    @Override
    public void disconnect() throws IOException {

    }

    @Override
    public void send(Send request) {
      if (request instanceof ReplicaMetadataRequest) {
        metadataRequest = (ReplicaMetadataRequest) request;
        host.replicaCountPerRequestTracker.add(metadataRequest.getReplicaMetadataRequestInfoList().size());
      } else if (request instanceof GetRequest) {
        getRequest = (GetRequest) request;
        buffersToReturn = new ArrayList<>();
        infosToReturn = new HashMap<>();
        messageMetadatasToReturn = new HashMap<>();
        boolean requestIsEmpty = true;
        for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
          PartitionId partitionId = partitionRequestInfo.getPartition();
          List<ByteBuffer> bufferList = host.buffersByPartition.get(partitionId);
          List<MessageInfo> messageInfoList = host.infosByPartition.get(partitionId);
          infosToReturn.put(partitionId, new ArrayList<>());
          messageMetadatasToReturn.put(partitionId, new ArrayList<>());
          List<StoreKey> convertedKeys = partitionRequestInfo.getBlobIds()
              .stream()
              .map(k -> conversionMap.getOrDefault(k, k))
              .collect(Collectors.toList());
          List<StoreKey> dedupKeys = convertedKeys.stream().distinct().collect(Collectors.toList());
          for (StoreKey key : dedupKeys) {
            requestIsEmpty = false;
            int index = 0;
            MessageInfo infoFound = null;
            for (MessageInfo info : messageInfoList) {
              if (key.equals(info.getStoreKey())) {
                infoFound = getMergedMessageInfo(info.getStoreKey(), messageInfoList);
                messageMetadatasToReturn.get(partitionId).add(null);
                buffersToReturn.add(bufferList.get(index));
                break;
              }
              index++;
            }
            if (infoFound != null) {
              // If MsgInfo says it is deleted, get the original Put Message's MessageInfo as that is what Get Request
              // looks for. Just set the deleted flag to true for the constructed MessageInfo from Put.
              if (infoFound.isDeleted()) {
                MessageInfo putMsgInfo = getMessageInfo(infoFound.getStoreKey(), messageInfoList, false, false, false);
                infoFound = new MessageInfo(putMsgInfo.getStoreKey(), putMsgInfo.getSize(), true, false, false,
                    putMsgInfo.getExpirationTimeInMs(), null, putMsgInfo.getAccountId(), putMsgInfo.getContainerId(),
                    putMsgInfo.getOperationTimeMs(), infoFound.getLifeVersion());
              } else if (infoFound.isUndeleted()) {
                MessageInfo putMsgInfo = getMessageInfo(infoFound.getStoreKey(), messageInfoList, false, false, false);
                infoFound = new MessageInfo(putMsgInfo.getStoreKey(), putMsgInfo.getSize(), false, false, true,
                    putMsgInfo.getExpirationTimeInMs(), null, putMsgInfo.getAccountId(), putMsgInfo.getContainerId(),
                    putMsgInfo.getOperationTimeMs(), infoFound.getLifeVersion());
              }
              infosToReturn.get(partitionId).add(infoFound);
            }
          }
        }
        assertFalse("Replication should not make empty GetRequests", requestIsEmpty);
      }
    }

    @Override
    public ChannelOutput receive() throws IOException {
      Response response;
      if (metadataRequest != null) {
        List<ReplicaMetadataResponseInfo> responseInfoList = new ArrayList<>();
        for (ReplicaMetadataRequestInfo requestInfo : metadataRequest.getReplicaMetadataRequestInfoList()) {
          List<MessageInfo> messageInfosToReturn = new ArrayList<>();
          List<MessageInfo> allMessageInfos = host.infosByPartition.get(requestInfo.getPartitionId());
          int startIndex = ((MockFindToken) (requestInfo.getToken())).getIndex();
          int endIndex = Math.min(allMessageInfos.size(), startIndex + maxSizeToReturn);
          int indexRequested = 0;
          Set<StoreKey> processedKeys = new HashSet<>();
          for (int i = startIndex; i < endIndex; i++) {
            StoreKey key = allMessageInfos.get(i).getStoreKey();
            if (processedKeys.add(key)) {
              messageInfosToReturn.add(getMergedMessageInfo(key, allMessageInfos));
            }
            indexRequested = i;
          }
          long bytesRead =
              allMessageInfos.subList(0, indexRequested + 1).stream().mapToLong(MessageInfo::getSize).sum();
          long total = allMessageInfos.stream().mapToLong(MessageInfo::getSize).sum();
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              new ReplicaMetadataResponseInfo(requestInfo.getPartitionId(), requestInfo.getReplicaType(),
                  new MockFindToken(indexRequested, bytesRead), messageInfosToReturn, total - bytesRead,
                  ReplicaMetadataResponse.getCompatibleResponseVersion(metadataRequest.getVersionId()));
          responseInfoList.add(replicaMetadataResponseInfo);
        }
        response = new ReplicaMetadataResponse(1, "replicametadata", ServerErrorCode.No_Error, responseInfoList,
            ReplicaMetadataResponse.getCompatibleResponseVersion(metadataRequest.getVersionId()));
        metadataRequest = null;
      } else {
        List<PartitionResponseInfo> responseInfoList = new ArrayList<>();
        for (PartitionRequestInfo requestInfo : getRequest.getPartitionInfoList()) {
          List<MessageInfo> infosForPartition = infosToReturn.get(requestInfo.getPartition());
          PartitionResponseInfo partitionResponseInfo;
          if (!getRequest.getGetOption().equals(GetOption.Include_All) && !getRequest.getGetOption()
              .equals(GetOption.Include_Deleted_Blobs) && infosForPartition.stream().anyMatch(MessageInfo::isDeleted)) {
            partitionResponseInfo = new PartitionResponseInfo(requestInfo.getPartition(), ServerErrorCode.Blob_Deleted);
          } else if (!getRequest.getGetOption().equals(GetOption.Include_All) && !getRequest.getGetOption()
              .equals(GetOption.Include_Expired_Blobs) && infosForPartition.stream().anyMatch(MessageInfo::isExpired)) {
            partitionResponseInfo = new PartitionResponseInfo(requestInfo.getPartition(), ServerErrorCode.Blob_Expired);
          } else {
            partitionResponseInfo =
                new PartitionResponseInfo(requestInfo.getPartition(), infosToReturn.get(requestInfo.getPartition()),
                    messageMetadatasToReturn.get(requestInfo.getPartition()));
          }
          responseInfoList.add(partitionResponseInfo);
        }
        response = new GetResponse(1, "replication", responseInfoList, new MockSend(buffersToReturn),
            ServerErrorCode.No_Error);
        getRequest = null;
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) response.sizeInBytes());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer);
      WritableByteChannel channel = Channels.newChannel(stream);
      while (!response.isSendComplete()) {
        response.writeTo(channel);
      }
      buffer.flip();
      buffer.getLong();
      return new ChannelOutput(new ByteBufferInputStream(buffer), buffer.remaining());
    }

    @Override
    public String getRemoteHost() {
      return host.dataNodeId.getHostname();
    }

    @Override
    public int getRemotePort() {
      return host.dataNodeId.getPort();
    }
  }
}
