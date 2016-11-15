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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataRequestInfo;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.protocol.Response;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;


public class ReplicationTest {

  class MockMessageReadSet implements MessageReadSet {

    List<ByteBuffer> bytebuffers;
    List<StoreKey> storeKeys;

    public MockMessageReadSet(List<ByteBuffer> bytebuffers, List<StoreKey> storeKeys) {
      this.bytebuffers = bytebuffers;
      this.storeKeys = storeKeys;
    }

    @Override
    public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
      ByteBuffer bufferToWrite = bytebuffers.get(0);
      bufferToWrite.position((int) relativeOffset);
      bufferToWrite.limit((int) Math.min(maxSize, bufferToWrite.capacity()));
      channel.write(bufferToWrite);
      bufferToWrite.position(0);
      bufferToWrite.limit(bufferToWrite.capacity());
      return Math.min(maxSize, bufferToWrite.capacity()) - relativeOffset;
    }

    @Override
    public int count() {
      return bytebuffers.size();
    }

    @Override
    public long sizeInBytes(int index) {
      return bytebuffers.get(0).limit();
    }

    @Override
    public StoreKey getKeyAt(int index) {
      return storeKeys.get(index);
    }
  }

  class MockStore implements Store {

    class ByteBufferWrite implements Write {

      private List<ByteBuffer> buflist;
      private int index = 0;

      public ByteBufferWrite(List<ByteBuffer> buf) {
        this.buflist = buf;
      }

      @Override
      public int appendFrom(ByteBuffer buffer) throws IOException {
        buflist.get(index).put(buffer);
        index++;
        return buffer.capacity();
      }

      @Override
      public void appendFrom(ReadableByteChannel channel, long size) throws IOException {
        int sizeRead = 0;
        while (sizeRead < size) {
          sizeRead += channel.read(buflist.get(index++));
        }
        // @TODO: Is this doing the right thing?
      }
    }

    DummyLog log;
    List<MessageInfo> messageInfoList;

    class DummyLog {
      private List<ByteBuffer> logInfo;
      private long endOffSet;

      public DummyLog(List<ByteBuffer> bufferList) {
        this.logInfo = bufferList;
      }

      public void appendData(ByteBuffer buffer) {
        logInfo.add(buffer);
        endOffSet += buffer.capacity();
      }

      public ByteBuffer getData(int index) {
        return logInfo.get(index);
      }

      public long getEndOffSet() {
        return endOffSet;
      }
    }

    public MockStore(List<MessageInfo> messageInfo, List<ByteBuffer> buffers) {
      if (messageInfo.size() != buffers.size()) {
        throw new IllegalArgumentException("message info size and buffer size does not match");
      }
      messageInfoList = messageInfo;
      log = new DummyLog(buffers);
    }

    @Override
    public void start() throws StoreException {

    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> getOptions) throws StoreException {
      List<MessageInfo> infoOutput = new ArrayList<MessageInfo>();
      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      List<StoreKey> keys = new ArrayList<StoreKey>();
      int index = 0;
      for (MessageInfo info : messageInfoList) {
        for (StoreKey id : ids) {
          if (info.getStoreKey().equals(id)) {
            infoOutput.add(info);
            buffers.add(log.getData(index));
            keys.add(info.getStoreKey());
          }
        }
        index++;
      }
      return new StoreInfo(new MockMessageReadSet(buffers, keys), messageInfoList);
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {
      List<MessageInfo> messageInfoListTemp = messageSetToWrite.getMessageSetInfo();
      List<ByteBuffer> buffersToWrite = new ArrayList<ByteBuffer>();
      for (MessageInfo messageInfo : messageInfoListTemp) {
        ByteBuffer buf = ByteBuffer.allocate((int) messageInfo.getSize());
        buffersToWrite.add(buf);
      }
      try {
        messageSetToWrite.writeTo(new ByteBufferWrite(buffersToWrite));
      } catch (IOException e) {

      }
      for (ByteBuffer buf : buffersToWrite) {
        buf.flip();
        log.appendData(buf);
      }
      messageInfoList.addAll(messageInfoListTemp);
    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
      int index = 0;
      MessageInfo messageInfoFound = null;
      for (MessageInfo messageInfo : messageInfoList) {
        if (messageInfo.getStoreKey().equals(messageSetToDelete.getMessageSetInfo().get(0).getStoreKey())) {
          messageInfoFound = messageInfo;
          break;
        }
        index++;
      }
      messageInfoList.set(index, new MessageInfo(messageInfoFound.getStoreKey(), messageInfoFound.getSize(), true,
          messageInfoFound.getExpirationTimeInMs()));
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxSizeOfEntries) throws StoreException {
      MockFindToken tokenmock = (MockFindToken) token;
      List<MessageInfo> entriesToReturn = new ArrayList<MessageInfo>();
      long currentSizeOfEntriesInBytes = 0;
      int index = 0;
      while (currentSizeOfEntriesInBytes < maxSizeOfEntries && index < messageInfoList.size()) {
        entriesToReturn.add(messageInfoList.get(tokenmock.getIndex() + index));
        currentSizeOfEntriesInBytes += messageInfoList.get(tokenmock.getIndex() + index).getSize();
        index++;
      }

      int startIndex = tokenmock.getIndex();
      int totalSizeRead = 0;
      for (int i = 0; i < startIndex; i++) {
        totalSizeRead += messageInfoList.get(i).getSize();
      }
      totalSizeRead += currentSizeOfEntriesInBytes;
      return new FindInfo(entriesToReturn,
          new MockFindToken(tokenmock.getIndex() + entriesToReturn.size(), totalSizeRead));
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
      Set<StoreKey> keysMissing = new HashSet<StoreKey>();
      for (StoreKey key : keys) {
        boolean found = false;
        for (MessageInfo messageInfo : messageInfoList) {
          if (messageInfo.getStoreKey().equals(key)) {
            found = true;
            break;
          }
        }
        if (!found) {
          keysMissing.add(key);
        }
      }
      return keysMissing;
    }

    @Override
    public boolean isKeyDeleted(StoreKey key) throws StoreException {
      for (MessageInfo messageInfo : messageInfoList) {
        if (messageInfo.getStoreKey().equals(key) && messageInfo.isDeleted()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public long getSizeInBytes() {
      return log.getEndOffSet();
    }

    @Override
    public void shutdown() throws StoreException {
    }
  }

  class MockConnection implements ConnectedChannel {

    class MockSend implements Send {

      private List<ByteBuffer> bufferList;
      private int index;
      private int size;

      public MockSend(List<ByteBuffer> bufferList) {
        this.bufferList = bufferList;
        index = 0;
        size = 0;
        for (ByteBuffer buffer : bufferList) {
          size += buffer.remaining();
        }
      }

      @Override
      public long writeTo(WritableByteChannel channel) throws IOException {
        long written = channel.write(bufferList.get(index));
        index++;
        return written;
      }

      @Override
      public boolean isSendComplete() {
        return bufferList.size() == index;
      }

      @Override
      public long sizeInBytes() {
        return size;
      }
    }

    Map<PartitionId, List<MessageInfo>> messageInfoForPartition;
    Map<PartitionId, List<ByteBuffer>> bufferListForPartition;
    List<ByteBuffer> bufferToReturn;
    Map<PartitionId, List<MessageInfo>> messageInfoToReturn;
    ReplicaMetadataRequest metadataRequest;
    GetRequest getRequest;
    String host;
    int port;
    int maxSizeToReturn;

    public MockConnection(String host, int port, Map<PartitionId, List<MessageInfo>> messageInfoList,
        Map<PartitionId, List<ByteBuffer>> bufferList, int maxSizeToReturn) {
      this.messageInfoForPartition = messageInfoList;
      this.bufferListForPartition = bufferList;
      this.host = host;
      this.port = port;
      this.maxSizeToReturn = maxSizeToReturn;
    }

    @Override
    public void send(Send request) throws IOException {
      if (request instanceof ReplicaMetadataRequest) {
        metadataRequest = (ReplicaMetadataRequest) request;
      }
      if (request instanceof GetRequest) {
        getRequest = (GetRequest) request;
        bufferToReturn = new ArrayList<ByteBuffer>();
        messageInfoToReturn = new HashMap<PartitionId, List<MessageInfo>>();
        for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
          PartitionId partitionId = partitionRequestInfo.getPartition();
          List<ByteBuffer> bufferList = bufferListForPartition.get(partitionId);
          List<MessageInfo> messageInfoList = messageInfoForPartition.get(partitionId);
          messageInfoToReturn.put(partitionId, new ArrayList<MessageInfo>());
          for (StoreKey key : partitionRequestInfo.getBlobIds()) {
            int index = 0;
            for (MessageInfo info : messageInfoList) {
              if (key.equals(info.getStoreKey())) {
                messageInfoToReturn.get(partitionId).add(info);
                bufferToReturn.add(bufferList.get(index));
              }
              index++;
            }
          }
        }
      }
    }

    @Override
    public ChannelOutput receive() throws IOException {
      Response response = null;
      if (metadataRequest != null) {
        List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList =
            new ArrayList<ReplicaMetadataResponseInfo>();
        for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : metadataRequest.getReplicaMetadataRequestInfoList()) {
          List<MessageInfo> messageInfoToReturn = new ArrayList<MessageInfo>();
          int startIndex = ((MockFindToken) (replicaMetadataRequestInfo.getToken())).getIndex();
          int endIndex = Math.min(messageInfoForPartition.get(replicaMetadataRequestInfo.getPartitionId()).size(),
              startIndex + maxSizeToReturn);
          int indexRequested = 0;
          for (int i = startIndex; i < endIndex; i++) {
            messageInfoToReturn.add(messageInfoForPartition.get(replicaMetadataRequestInfo.getPartitionId()).get(i));
            indexRequested = i;
          }
          ReplicaMetadataResponseInfo replicaMetadataResponseInfo =
              new ReplicaMetadataResponseInfo(replicaMetadataRequestInfo.getPartitionId(),
                  new MockFindToken(indexRequested, replicaMetadataRequestInfo.getToken().getBytesRead()),
                  messageInfoToReturn, 0);
          replicaMetadataResponseInfoList.add(replicaMetadataResponseInfo);
        }
        response = new ReplicaMetadataResponse(1, "replicametadata", ServerErrorCode.No_Error,
            replicaMetadataResponseInfoList);
        metadataRequest = null;
      } else {
        List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
        for (PartitionRequestInfo partitionRequestInfo : getRequest.getPartitionInfoList()) {
          PartitionResponseInfo partitionResponseInfo = new PartitionResponseInfo(partitionRequestInfo.getPartition(),
              messageInfoToReturn.get(partitionRequestInfo.getPartition()));
          partitionResponseInfoList.add(partitionResponseInfo);
        }
        response = new GetResponse(1, "replication", partitionResponseInfoList, new MockSend(bufferToReturn),
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
      return host;
    }

    @Override
    public int getRemotePort() {
      return port;
    }
  }

  class MockConnectionPool implements ConnectionPool {

    Map<String, Map<PartitionId, List<MessageInfo>>> messageInfoList;
    Map<String, Map<PartitionId, List<ByteBuffer>>> byteBufferList;
    int maxEntriesToReturn;

    public MockConnectionPool(Map<String, Map<PartitionId, List<MessageInfo>>> messageInfoList,
        Map<String, Map<PartitionId, List<ByteBuffer>>> byteBufferList, int maxEntriesToReturn) {
      this.messageInfoList = messageInfoList;
      this.byteBufferList = byteBufferList;
      this.maxEntriesToReturn = maxEntriesToReturn;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public ConnectedChannel checkOutConnection(String host, Port port, long timeout)
        throws IOException, InterruptedException, ConnectionPoolTimeoutException {
      return new MockConnection(host, port.getPort(), messageInfoList.get(host + port), byteBufferList.get(host + port),
          maxEntriesToReturn);
    }

    @Override
    public void checkInConnection(ConnectedChannel connectedChannel) {
    }

    @Override
    public void destroyConnection(ConnectedChannel connectedChannel) {
    }
  }

  @Test
  public void replicaThreadTest() throws InterruptedException, IOException {
    try {
      Random random = new Random();
      MockClusterMap clusterMap = new MockClusterMap();
      DataNodeId dataNode1 = clusterMap.getDataNodeIds().get(0);
      DataNodeId dataNode2 = clusterMap.getDataNodeIds().get(1);

      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(clusterMap.getDataNodeId("localhost", dataNode1.getPort()));

      Map<String, Map<PartitionId, List<MessageInfo>>> replicaStores =
          new HashMap<String, Map<PartitionId, List<MessageInfo>>>();
      Map<String, Map<PartitionId, List<ByteBuffer>>> replicaBuffers =
          new HashMap<String, Map<PartitionId, List<ByteBuffer>>>();

      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      Map<PartitionId, List<MessageInfo>> messageInfoNode1 = new HashMap<PartitionId, List<MessageInfo>>();
      Map<PartitionId, List<MessageInfo>> messageInfoNode2 = new HashMap<PartitionId, List<MessageInfo>>();
      Map<PartitionId, List<ByteBuffer>> bufferListNode1 = new HashMap<PartitionId, List<ByteBuffer>>();
      Map<PartitionId, List<ByteBuffer>> bufferListNode2 = new HashMap<PartitionId, List<ByteBuffer>>();
      for (int i = 0; i < partitionIds.size(); i++) {
        List<MessageInfo> messageInfoListLocalReplica = new ArrayList<MessageInfo>();
        List<ByteBuffer> messageBufferListLocalReplica = new ArrayList<ByteBuffer>();

        List<MessageInfo> messageInfoListRemoteReplica2 = new ArrayList<MessageInfo>();
        List<ByteBuffer> messageBufferListLocalReplica2 = new ArrayList<ByteBuffer>();

        for (int j = 0; j < 10; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          ByteBuffer byteBuffer = constructTestBlobInMessageFormat(id, 1000, random);
          long streamSize = byteBuffer.limit();
          messageInfoListLocalReplica.add(new MessageInfo(id, streamSize));
          messageInfoListRemoteReplica2.add(new MessageInfo(id, streamSize));
          messageBufferListLocalReplica.add(byteBuffer);
          messageBufferListLocalReplica2.add(byteBuffer);
        }

        // add additional messages to replica 2
        for (int j = 10; j < 15; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          ByteBuffer byteBuffer = constructTestBlobInMessageFormat(id, 1000, random);
          long streamSize = byteBuffer.limit();
          messageInfoListRemoteReplica2.add(new MessageInfo(id, streamSize));
          messageBufferListLocalReplica2.add(byteBuffer);
        }

        // add an expired message to replica 2
        BlobId idExpired = new BlobId(partitionIds.get(i));
        ByteBuffer byteBuffer = constructTestBlobInMessageFormat(idExpired, 1000, random);
        long streamSize = byteBuffer.limit();
        messageInfoListRemoteReplica2.add(new MessageInfo(idExpired, streamSize, 1));
        messageBufferListLocalReplica2.add(byteBuffer);
        messageInfoNode1.put(partitionIds.get(i), messageInfoListLocalReplica);
        bufferListNode1.put(partitionIds.get(i), messageBufferListLocalReplica);
        messageInfoNode2.put(partitionIds.get(i), messageInfoListRemoteReplica2);
        bufferListNode2.put(partitionIds.get(i), messageBufferListLocalReplica2);
      }
      replicaStores.put("localhost" + dataNode2.getPort(), messageInfoNode2);
      replicaBuffers.put("localhost" + dataNode2.getPort(), bufferListNode2);

      List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
      for (ReplicaId replicaId : replicaIds) {
        for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
          RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(peerReplicaId, replicaId,
              new MockStore(messageInfoNode1.get(replicaId.getPartitionId()),
                  bufferListNode1.get(replicaId.getPartitionId())), new MockFindToken(0, 0), 1000000,
              SystemTime.getInstance(), new Port(peerReplicaId.getDataNodeId().getPort(), PortType.PLAINTEXT));
          remoteReplicas.add(remoteReplicaInfo);
        }
      }

      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
      DataNodeId dataNodeId = null;
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
        if (remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() == dataNode2.getPort()) {
          dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
          List<RemoteReplicaInfo> remoteReplicaInfoList =
              replicasToReplicate.get(remoteReplicaInfo.getReplicaId().getDataNodeId());
          if (remoteReplicaInfoList != null) {
            remoteReplicaInfoList.add(remoteReplicaInfo);
          } else {
            remoteReplicaInfoList = new ArrayList<RemoteReplicaInfo>();
            remoteReplicaInfoList.add(remoteReplicaInfo);
            replicasToReplicate.put(remoteReplicaInfo.getReplicaId().getDataNodeId(), remoteReplicaInfoList);
          }
        }
      }
      ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(new Properties()));

      Map<String, ArrayList<ReplicaThread>> replicaThreadMap = new HashMap<String, ArrayList<ReplicaThread>>();
      replicaThreadMap.put("localhost", new ArrayList<ReplicaThread>());
      ReplicationMetrics replicationMetrics = new ReplicationMetrics(new MetricRegistry(), replicaIds);
      replicationMetrics.populatePerColoMetrics(new HashSet<String>(Arrays.asList("localhost")));
      StoreKeyFactory storeKeyFactory = null;
      try {
        storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
      } catch (Exception e) {
        System.out.println("Error creating StoreKeyFactory ");
        throw new IOException("Error creating StoreKeyFactory " + e);
      }

      ReplicaThread replicaThread =
          new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
              new AtomicInteger(0), clusterMap.getDataNodeId("localhost", dataNode1.getPort()),
              new MockConnectionPool(replicaStores, replicaBuffers, 3), config, replicationMetrics, null,
              storeKeyFactory, true, clusterMap.getMetricRegistry(), false, "localhost",
              new ResponseHandler(clusterMap));
      List<ReplicaThread.ExchangeMetadataResponse> response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 5), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 4);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 5), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 8);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 2);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 11);
      }

      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 3);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 14);
      }
      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 15);
      }

      //check replica1 store is the same as replica 2 store in messageinfo and byte buffers
      for (Map.Entry<PartitionId, List<MessageInfo>> entry : messageInfoNode2.entrySet()) {
        for (MessageInfo messageInfo : entry.getValue()) {
          boolean found = false;
          for (MessageInfo messageInfo1 : messageInfoNode1.get(entry.getKey())) {
            if (messageInfo.getStoreKey().equals(messageInfo1.getStoreKey())) {
              found = true;
              break;
            }
          }
          if (!found) {
            Assert.assertTrue(messageInfo.isExpired());
          }
        }
      }
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : bufferListNode2.entrySet()) {
        int totalFound = 0;
        for (ByteBuffer buf : entry.getValue()) {
          for (ByteBuffer bufActual : bufferListNode1.get(entry.getKey())) {
            if (Arrays.equals(buf.array(), bufActual.array())) {
              totalFound++;
              break;
            }
          }
        }
        Assert.assertEquals(totalFound, entry.getValue().size() - 1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void replicaTokenTest() throws InterruptedException {
    final long tokenPersistInterval = 100;
    Time time = new MockTime();
    MockFindToken token1 = new MockFindToken(0, 0);
    RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(new MockReplicaId(), new MockReplicaId(),
        new MockStore(new ArrayList<MessageInfo>(), new ArrayList<ByteBuffer>()), token1, tokenPersistInterval, time,
        new Port(5000, PortType.PLAINTEXT));

    // The equality check is for the reference, which is fine.
    // Initially, the current token and the token to persist are the same.
    Assert.assertEquals(remoteReplicaInfo.getToken(), token1);
    Assert.assertEquals(remoteReplicaInfo.getTokenToPersist(), token1);
    MockFindToken token2 = new MockFindToken(100, 100);

    remoteReplicaInfo.initializeTokens(token2);
    // Both tokens should be the newly initialized token.
    Assert.assertEquals(remoteReplicaInfo.getToken(), token2);
    Assert.assertEquals(remoteReplicaInfo.getTokenToPersist(), token2);
    remoteReplicaInfo.onTokenPersisted();

    MockFindToken token3 = new MockFindToken(200, 200);

    remoteReplicaInfo.setToken(token3);
    // Token to persist should still be the old token.
    Assert.assertEquals(remoteReplicaInfo.getToken(), token3);
    Assert.assertEquals(remoteReplicaInfo.getTokenToPersist(), token2);
    remoteReplicaInfo.onTokenPersisted();

    // Sleep for shorter than token persist interval.
    time.sleep(tokenPersistInterval - 1);
    // Token to persist should still be the old token.
    Assert.assertEquals(remoteReplicaInfo.getToken(), token3);
    Assert.assertEquals(remoteReplicaInfo.getTokenToPersist(), token2);
    remoteReplicaInfo.onTokenPersisted();

    MockFindToken token4 = new MockFindToken(200, 200);
    remoteReplicaInfo.setToken(token4);

    time.sleep(2);
    // Token to persist should be the most recent token as of currentTime - tokenToPersistInterval
    // which is token3 at this time.
    Assert.assertEquals(remoteReplicaInfo.getToken(), token4);
    Assert.assertEquals(remoteReplicaInfo.getTokenToPersist(), token3);
    remoteReplicaInfo.onTokenPersisted();

    time.sleep(tokenPersistInterval + 1);
    // The most recently set token as of currentTime - tokenToPersistInterval is token4
    Assert.assertEquals(remoteReplicaInfo.getToken(), token4);
    Assert.assertEquals(remoteReplicaInfo.getTokenToPersist(), token4);
    remoteReplicaInfo.onTokenPersisted();
  }

  @Test
  public void replicaThreadTestForExpiredBlobs() throws InterruptedException, IOException {
    try {
      Random random = new Random();
      MockClusterMap clusterMap = new MockClusterMap();
      DataNodeId dataNode1 = clusterMap.getDataNodeIds().get(0);
      DataNodeId dataNode2 = clusterMap.getDataNodeIds().get(1);

      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(clusterMap.getDataNodeId("localhost", dataNode1.getPort()));

      Map<String, Map<PartitionId, List<MessageInfo>>> replicaStores =
          new HashMap<String, Map<PartitionId, List<MessageInfo>>>();
      Map<String, Map<PartitionId, List<ByteBuffer>>> replicaBuffers =
          new HashMap<String, Map<PartitionId, List<ByteBuffer>>>();

      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      Map<PartitionId, List<MessageInfo>> messageInfoNode1 = new HashMap<PartitionId, List<MessageInfo>>();
      Map<PartitionId, List<MessageInfo>> messageInfoNode2 = new HashMap<PartitionId, List<MessageInfo>>();
      Map<PartitionId, List<ByteBuffer>> bufferListNode1 = new HashMap<PartitionId, List<ByteBuffer>>();
      Map<PartitionId, List<ByteBuffer>> bufferListNode2 = new HashMap<PartitionId, List<ByteBuffer>>();
      for (int i = 0; i < partitionIds.size(); i++) {
        List<MessageInfo> messageInfoListLocalReplica = new ArrayList<MessageInfo>();
        List<ByteBuffer> messageBufferListLocalReplica = new ArrayList<ByteBuffer>();

        List<MessageInfo> messageInfoListRemoteReplica2 = new ArrayList<MessageInfo>();
        List<ByteBuffer> messageBufferListLocalReplica2 = new ArrayList<ByteBuffer>();

        for (int j = 0; j < 10; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          ByteBuffer byteBuffer = constructTestBlobInMessageFormat(id, 1000, random);
          long streamSize = byteBuffer.limit();
          messageInfoListLocalReplica.add(new MessageInfo(id, streamSize));
          messageInfoListRemoteReplica2.add(new MessageInfo(id, streamSize));
          messageBufferListLocalReplica.add(byteBuffer);
          messageBufferListLocalReplica2.add(byteBuffer);
        }

        // add an expired message to replica 2
        BlobId idExpired = new BlobId(partitionIds.get(i));
        ByteBuffer byteBuffer = constructTestBlobInMessageFormat(idExpired, 1000, random);
        long streamSize = byteBuffer.limit();
        messageInfoListRemoteReplica2.add(new MessageInfo(idExpired, streamSize, 1));
        messageBufferListLocalReplica2.add(byteBuffer);
        messageInfoNode1.put(partitionIds.get(i), messageInfoListLocalReplica);
        bufferListNode1.put(partitionIds.get(i), messageBufferListLocalReplica);

        // add additional messages to replica 2
        for (int j = 10; j < 15; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          byteBuffer = constructTestBlobInMessageFormat(id, 1000, random);
          streamSize = byteBuffer.limit();
          messageInfoListRemoteReplica2.add(new MessageInfo(id, streamSize));
          messageBufferListLocalReplica2.add(byteBuffer);
        }

        messageInfoNode2.put(partitionIds.get(i), messageInfoListRemoteReplica2);
        bufferListNode2.put(partitionIds.get(i), messageBufferListLocalReplica2);
      }
      replicaStores.put("localhost" + dataNode2.getPort(), messageInfoNode2);
      replicaBuffers.put("localhost" + dataNode2.getPort(), bufferListNode2);

      List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
      for (ReplicaId replicaId : replicaIds) {
        for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
          RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(peerReplicaId, replicaId,
              new MockStore(messageInfoNode1.get(replicaId.getPartitionId()),
                  bufferListNode1.get(replicaId.getPartitionId())), new MockFindToken(0, 0), 1000000,
              SystemTime.getInstance(), new Port(peerReplicaId.getDataNodeId().getPort(), PortType.PLAINTEXT));
          remoteReplicas.add(remoteReplicaInfo);
        }
      }

      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
      DataNodeId dataNodeId = null;
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
        if (remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() == dataNode2.getPort()) {
          dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
          List<RemoteReplicaInfo> remoteReplicaInfoList =
              replicasToReplicate.get(remoteReplicaInfo.getReplicaId().getDataNodeId());
          if (remoteReplicaInfoList != null) {
            remoteReplicaInfoList.add(remoteReplicaInfo);
          } else {
            remoteReplicaInfoList = new ArrayList<RemoteReplicaInfo>();
            remoteReplicaInfoList.add(remoteReplicaInfo);
            replicasToReplicate.put(remoteReplicaInfo.getReplicaId().getDataNodeId(), remoteReplicaInfoList);
          }
        }
      }
      ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(new Properties()));
      Map<String, ArrayList<ReplicaThread>> replicaThreadMap = new HashMap<String, ArrayList<ReplicaThread>>();
      replicaThreadMap.put("localhost", new ArrayList<ReplicaThread>());
      ReplicationMetrics replicationMetrics = new ReplicationMetrics(new MetricRegistry(), replicaIds);
      replicationMetrics.populatePerColoMetrics(new HashSet<String>(Arrays.asList("localhost")));
      StoreKeyFactory storeKeyFactory = null;
      try {
        storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
      } catch (Exception e) {
        System.out.println("Error creating StoreKeyFactory ");
        throw new IOException("Error creating StoreKeyFactory " + e);
      }

      ReplicaThread replicaThread =
          new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
              new AtomicInteger(0), clusterMap.getDataNodeId("localhost", dataNode1.getPort()),
              new MockConnectionPool(replicaStores, replicaBuffers, 3), config, replicationMetrics, null,
              storeKeyFactory, true, clusterMap.getMetricRegistry(), false, "localhost",
              new ResponseHandler(clusterMap));
      List<ReplicaThread.ExchangeMetadataResponse> response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 5), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 4);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 5), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 8);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 1);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 11);
      }

      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 3);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 14);
      }
      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 1);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 15);
      }

      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 15);
      }

      //check replica1 store is the same as replica 2 store in messageinfo and byte buffers
      for (Map.Entry<PartitionId, List<MessageInfo>> entry : messageInfoNode2.entrySet()) {
        for (MessageInfo messageInfo : entry.getValue()) {
          boolean found = false;
          for (MessageInfo messageInfo1 : messageInfoNode1.get(entry.getKey())) {
            if (messageInfo.getStoreKey().equals(messageInfo1.getStoreKey())) {
              found = true;
              break;
            }
          }
          if (!found) {
            Assert.assertTrue(messageInfo.isExpired());
          }
        }
      }
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : bufferListNode2.entrySet()) {
        int totalFound = 0;
        for (ByteBuffer buf : entry.getValue()) {
          for (ByteBuffer bufActual : bufferListNode1.get(entry.getKey())) {
            if (Arrays.equals(buf.array(), bufActual.array())) {
              totalFound++;
              break;
            }
          }
        }
        Assert.assertEquals(totalFound, entry.getValue().size() - 1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void replicaThreadTestWithCorruptMessages() throws InterruptedException, IOException {
    try {
      Random random = new Random();
      MockClusterMap clusterMap = new MockClusterMap();
      DataNodeId dataNode1 = clusterMap.getDataNodeIds().get(0);
      DataNodeId dataNode2 = clusterMap.getDataNodeIds().get(1);

      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(clusterMap.getDataNodeId("localhost", dataNode1.getPort()));

      Map<String, Map<PartitionId, List<MessageInfo>>> replicaStores =
          new HashMap<String, Map<PartitionId, List<MessageInfo>>>();
      Map<String, Map<PartitionId, List<ByteBuffer>>> replicaBuffers =
          new HashMap<String, Map<PartitionId, List<ByteBuffer>>>();

      List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
      Map<PartitionId, List<MessageInfo>> messageInfoNode1 = new HashMap<PartitionId, List<MessageInfo>>();
      Map<PartitionId, List<MessageInfo>> messageInfoNode2 = new HashMap<PartitionId, List<MessageInfo>>();
      Map<PartitionId, List<ByteBuffer>> bufferListNode1 = new HashMap<PartitionId, List<ByteBuffer>>();
      Map<PartitionId, List<ByteBuffer>> bufferListNode2 = new HashMap<PartitionId, List<ByteBuffer>>();

      Map<PartitionId, BlobId> partitionIdToCorruptIdMap = new HashMap<PartitionId, BlobId>();

      for (int i = 0; i < partitionIds.size(); i++) {
        List<MessageInfo> messageInfoListLocalReplica = new ArrayList<MessageInfo>();
        List<ByteBuffer> messageBufferListLocalReplica = new ArrayList<ByteBuffer>();

        List<MessageInfo> messageInfoListRemoteReplica2 = new ArrayList<MessageInfo>();
        List<ByteBuffer> messageBufferListRemoteReplica2 = new ArrayList<ByteBuffer>();

        for (int j = 0; j < 10; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          ByteBuffer byteBuffer = constructTestBlobInMessageFormat(id, 1000, random);
          long streamSize = byteBuffer.limit();
          messageInfoListLocalReplica.add(new MessageInfo(id, streamSize));
          messageInfoListRemoteReplica2.add(new MessageInfo(id, streamSize));
          messageBufferListLocalReplica.add(byteBuffer);
          messageBufferListRemoteReplica2.add(byteBuffer);
        }

        // add a corrupt message to replica 2
        BlobId corruptId = new BlobId(partitionIds.get(i));
        ByteBuffer corruptByteBuffer = constructTestBlobInMessageFormat(corruptId, 1000, random);
        byte[] data = corruptByteBuffer.array();
        new Random().nextBytes(data);
        long corruptStreamSize = corruptByteBuffer.limit();
        messageInfoListRemoteReplica2.add(new MessageInfo(corruptId, corruptStreamSize));
        messageBufferListRemoteReplica2.add(corruptByteBuffer);
        partitionIdToCorruptIdMap.put(partitionIds.get(i), corruptId);

        // add additional messages to replica 2
        for (int j = 10; j < 15; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          ByteBuffer byteBuffer = constructTestBlobInMessageFormat(id, 1000, random);
          long streamSize = byteBuffer.limit();
          messageInfoListRemoteReplica2.add(new MessageInfo(id, streamSize));
          messageBufferListRemoteReplica2.add(byteBuffer);
        }

        // add an expired message to replica 2
        BlobId idExpired = new BlobId(partitionIds.get(i));
        ByteBuffer byteBuffer = constructTestBlobInMessageFormat(idExpired, 1000, random);
        long streamSize = byteBuffer.limit();
        messageInfoListRemoteReplica2.add(new MessageInfo(idExpired, streamSize, 1));
        messageBufferListRemoteReplica2.add(byteBuffer);

        messageInfoNode1.put(partitionIds.get(i), messageInfoListLocalReplica);
        bufferListNode1.put(partitionIds.get(i), messageBufferListLocalReplica);
        messageInfoNode2.put(partitionIds.get(i), messageInfoListRemoteReplica2);
        bufferListNode2.put(partitionIds.get(i), messageBufferListRemoteReplica2);
      }
      replicaStores.put("localhost" + dataNode2.getPort(), messageInfoNode2);
      replicaBuffers.put("localhost" + dataNode2.getPort(), bufferListNode2);

      List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
      for (ReplicaId replicaId : replicaIds) {
        for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
          RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(peerReplicaId, replicaId,
              new MockStore(messageInfoNode1.get(replicaId.getPartitionId()),
                  bufferListNode1.get(replicaId.getPartitionId())), new MockFindToken(0, 0), 1000000,
              SystemTime.getInstance(), new Port(peerReplicaId.getDataNodeId().getPort(), PortType.PLAINTEXT));
          remoteReplicas.add(remoteReplicaInfo);
        }
      }

      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
      DataNodeId dataNodeId = null;
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
        if (remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() == dataNode2.getPort()) {
          dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
          List<RemoteReplicaInfo> remoteReplicaInfoList =
              replicasToReplicate.get(remoteReplicaInfo.getReplicaId().getDataNodeId());
          if (remoteReplicaInfoList != null) {
            remoteReplicaInfoList.add(remoteReplicaInfo);
          } else {
            remoteReplicaInfoList = new ArrayList<RemoteReplicaInfo>();
            remoteReplicaInfoList.add(remoteReplicaInfo);
            replicasToReplicate.put(remoteReplicaInfo.getReplicaId().getDataNodeId(), remoteReplicaInfoList);
          }
        }
      }
      ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(new Properties()));
      Map<String, ArrayList<ReplicaThread>> replicaThreadMap = new HashMap<String, ArrayList<ReplicaThread>>();
      replicaThreadMap.put("localhost", new ArrayList<ReplicaThread>());
      ReplicationMetrics replicationMetrics = new ReplicationMetrics(new MetricRegistry(), replicaIds);
      replicationMetrics.populatePerColoMetrics(new HashSet<String>(Arrays.asList("localhost")));
      StoreKeyFactory storeKeyFactory = null;
      try {
        storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
      } catch (Exception e) {
        System.out.println("Error creating StoreKeyFactory ");
        throw new IOException("Error creating StoreKeyFactory " + e);
      }

      ReplicaThread replicaThread =
          new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
              new AtomicInteger(0), clusterMap.getDataNodeId("localhost", dataNode1.getPort()),
              new MockConnectionPool(replicaStores, replicaBuffers, 3), config, replicationMetrics, null,
              storeKeyFactory, true, clusterMap.getMetricRegistry(), false, "localhost",
              new ResponseHandler(clusterMap));
      List<ReplicaThread.ExchangeMetadataResponse> response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 5), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 4);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 5), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 8);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        // one message is corrupt
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 2);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 11);
      }

      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 3);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 14);
      }
      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        // one message is corrupt
        Assert.assertTrue(response.get(i).missingStoreKeys.size() == 1);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 16);
      }

      replicaThread.fixMissingStoreKeys(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId), response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", dataNode2.getPort(), replicaStores.get("localhost" + dataNode2.getPort()),
              replicaBuffers.get("localhost" + dataNode2.getPort()), 4), replicasToReplicate.get(dataNodeId));
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertTrue(response.get(i).missingStoreKeys.size() == 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 16);
      }

      //check replica1 store is the same as replica 2 store in messageinfo and byte buffers
      for (Map.Entry<PartitionId, List<MessageInfo>> entry : messageInfoNode2.entrySet()) {
        PartitionId partitionId = entry.getKey();
        for (MessageInfo messageInfo : entry.getValue()) {
          boolean found = false;
          for (MessageInfo messageInfo1 : messageInfoNode1.get(entry.getKey())) {
            if (messageInfo.getStoreKey().equals(messageInfo1.getStoreKey())) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (!messageInfo.isExpired() && !(messageInfo.getStoreKey()
                .equals(partitionIdToCorruptIdMap.get(partitionId)))) {
              Assert.assertFalse("Message is neither expired nor corrupt " + messageInfo, false);
            }
          }
        }
      }
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : bufferListNode2.entrySet()) {
        int totalFound = 0;
        for (ByteBuffer buf : entry.getValue()) {
          for (ByteBuffer bufActual : bufferListNode1.get(entry.getKey())) {
            if (Arrays.equals(buf.array(), bufActual.array())) {
              totalFound++;
              break;
            }
          }
        }
        Assert.assertEquals(totalFound, entry.getValue().size() - 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  private ByteBuffer constructTestBlobInMessageFormat(BlobId id, long blobSize, Random random)
      throws MessageFormatException, IOException {
    return constructEntireMessageForTestBlob(id, blobSize, random, "test");
  }

  /**
   * To construct an entire message with header, blob property, usermeta data and blob content
   * @param id Blob id for which the message has to be constructed
   * @param blobSize the size of the blob
   * @param random Random object used to generated usermetadata size
   * @param serviceId SerivceId used in blob properties
   * @return ByteBuffer representing the entire message
   * @throws MessageFormatException
   * @throws IOException
   */

  private ByteBuffer constructEntireMessageForTestBlob(BlobId id, long blobSize, Random random, String serviceId)
      throws MessageFormatException, IOException {

    int userMetadataSize = random.nextInt((int) blobSize / 2);
    byte[] usermetadata = new byte[userMetadataSize];
    byte[] blob = new byte[(int) blobSize];
    new Random().nextBytes(usermetadata);
    new Random().nextBytes(blob);
    BlobProperties blobProperties = new BlobProperties(blobSize, serviceId);

    MessageFormatInputStream inputStream =
        new PutMessageFormatInputStream(id, blobProperties, ByteBuffer.wrap(usermetadata),
            new ByteBufferInputStream(ByteBuffer.wrap(blob)), (int) blobSize);
    int streamSize = (int) inputStream.getSize();
    byte[] entireBlob = new byte[streamSize];
    inputStream.read(entireBlob, 0, streamSize);
    return ByteBuffer.wrap(entireBlob);
  }
}

