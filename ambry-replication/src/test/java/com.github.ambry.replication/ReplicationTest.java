package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNode;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Send;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ChannelOutput;
import com.github.ambry.shared.ConnectedChannel;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.ConnectionPoolTimeoutException;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PartitionRequestInfo;
import com.github.ambry.shared.PartitionResponseInfo;
import com.github.ambry.shared.ReplicaMetadataRequest;
import com.github.ambry.shared.ReplicaMetadataRequestInfo;
import com.github.ambry.shared.ReplicaMetadataResponse;
import com.github.ambry.shared.ReplicaMetadataResponseInfo;
import com.github.ambry.shared.Response;
import com.github.ambry.shared.ServerErrorCode;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageStoreRecovery;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreManager;
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Scheduler;
import java.util.EnumSet;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;


public class ReplicationTest {

  class MockMessageReadSet implements MessageReadSet {

    List<ByteBuffer> bytebuffers;
    List<StoreKey> storeKeys;

    public MockMessageReadSet(List<ByteBuffer> bytebuffers, List<StoreKey> storeKeys) {
      this.bytebuffers = bytebuffers;
      this.storeKeys = storeKeys;
    }

    @Override
    public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize)
        throws IOException {
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
      public int appendFrom(ByteBuffer buffer)
          throws IOException {
        buflist.get(index).put(buffer);
        index++;
        return buffer.capacity();
      }

      @Override
      public void appendFrom(ReadableByteChannel channel, long size)
          throws IOException {
        int sizeRead = 0;
        while(sizeRead < size) {
          sizeRead += channel.read(buflist.get(index++));
        }
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
    public void start()
        throws StoreException {

    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> getOptions)
        throws StoreException {
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
    public void put(MessageWriteSet messageSetToWrite)
        throws StoreException {
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
    public void delete(MessageWriteSet messageSetToDelete)
        throws StoreException {
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
    public FindInfo findEntriesSince(FindToken token, long maxSizeOfEntries)
        throws StoreException {
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
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys)
        throws StoreException {
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
    public boolean isKeyDeleted(StoreKey key)
        throws StoreException {
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
    public void shutdown()
        throws StoreException {
    }
  }

  class MockStoreManager extends StoreManager {
    Map<PartitionId, MockStore> stores;

    public MockStoreManager(StoreConfig config, Scheduler scheduler, MetricRegistry registry, List<ReplicaId> replicas,
        StoreKeyFactory factory, MessageStoreRecovery recovery, Map<PartitionId, MockStore> stores) {
      super(config, scheduler, registry, replicas, factory, recovery);
      this.stores = stores;
    }

    public Store getStore(PartitionId id) {
      return stores.get(id);
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
      public void writeTo(WritableByteChannel channel)
          throws IOException {
        channel.write(bufferList.get(index));
        index++;
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
    public void send(Send request)
        throws IOException {
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
    public ChannelOutput receive()
        throws IOException {
      Response response = null;
      if (metadataRequest != null) {
        List<ReplicaMetadataResponseInfo> replicaMetadataResponseInfoList =
            new ArrayList<ReplicaMetadataResponseInfo>();
        for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : metadataRequest
            .getReplicaMetadataRequestInfoList()) {
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
    public ConnectedChannel checkOutConnection(String host, int port, long timeout)
        throws IOException, InterruptedException, ConnectionPoolTimeoutException {
      return new MockConnection(host, port, messageInfoList.get(host + port), byteBufferList.get(host + port),
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
  public void replicaThreadTest()
      throws InterruptedException, IOException {
    try {
      MockClusterMap clusterMap = new MockClusterMap();
      List<ReplicaId> replicaIds = clusterMap.getReplicaIds(clusterMap.getDataNodeId("localhost", 64422));

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
          messageInfoListLocalReplica.add(new MessageInfo(id, 1000));
          messageInfoListRemoteReplica2.add(new MessageInfo(id, 1000));
          byte[] bytes = new byte[1000];
          new Random().nextBytes(bytes);
          messageBufferListLocalReplica.add(ByteBuffer.wrap(bytes));
          messageBufferListLocalReplica2.add(ByteBuffer.wrap(bytes));
        }

        // add additional messages to replica 2
        for (int j = 10; j < 15; j++) {
          BlobId id = new BlobId(partitionIds.get(i));
          messageInfoListRemoteReplica2.add(new MessageInfo(id, 1000));
          byte[] bytes = new byte[1000];
          new Random().nextBytes(bytes);
          messageBufferListLocalReplica2.add(ByteBuffer.wrap(bytes));
        }

        // add an expired message to replica 2
        BlobId idExpired = new BlobId(partitionIds.get(i));
        messageInfoListRemoteReplica2.add(new MessageInfo(idExpired, 1000, 1));
        byte[] bytesExpired = new byte[1000];
        new Random().nextBytes(bytesExpired);
        messageBufferListLocalReplica2.add(ByteBuffer.wrap(bytesExpired));
        messageInfoNode1.put(partitionIds.get(i), messageInfoListLocalReplica);
        bufferListNode1.put(partitionIds.get(i), messageBufferListLocalReplica);
        messageInfoNode2.put(partitionIds.get(i), messageInfoListRemoteReplica2);
        bufferListNode2.put(partitionIds.get(i), messageBufferListLocalReplica2);
      }
      replicaStores.put("localhost" + 64423, messageInfoNode2);
      replicaBuffers.put("localhost" + 64423, bufferListNode2);

      List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
      for (ReplicaId replicaId : replicaIds) {
        for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
          RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(peerReplicaId, replicaId,
              new MockStore(messageInfoNode1.get(replicaId.getPartitionId()),
                  bufferListNode1.get(replicaId.getPartitionId())), new MockFindToken(0, 0), 1000000);
          remoteReplicas.add(remoteReplicaInfo);
        }
      }

      Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = new HashMap<DataNodeId, List<RemoteReplicaInfo>>();
      DataNodeId dataNodeId = null;
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
        if (remoteReplicaInfo.getReplicaId().getDataNodeId().getPort() == 64423) {
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

      ReplicationMetrics replicationMetrics =
          new ReplicationMetrics(new MetricRegistry(), new ArrayList<ReplicaThread>(), new ArrayList<ReplicaThread>());
      ReplicaThread replicaThread =
          new ReplicaThread("threadtest", replicasToReplicate, new MockFindTokenFactory(), clusterMap,
              new AtomicInteger(0), clusterMap.getDataNodeId("localhost", 64422),
              new MockConnectionPool(replicaStores, replicaBuffers, 3), config, replicationMetrics, null);
      List<ReplicaThread.ExchangeMetadataResponse> response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
              replicaBuffers.get("localhost" + 64423), 5), replicasToReplicate.get(dataNodeId), false);
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 4);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
              replicaBuffers.get("localhost" + 64423), 5), replicasToReplicate.get(dataNodeId), false);
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 0);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 8);
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
              replicaBuffers.get("localhost" + 64423), 4), replicasToReplicate.get(dataNodeId), false);
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 2);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 11);
      }

      replicaThread.fixMissingStoreKeys(new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
          replicaBuffers.get("localhost" + 64423), 4), replicasToReplicate.get(dataNodeId), false, response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
              replicaBuffers.get("localhost" + 64423), 4), replicasToReplicate.get(dataNodeId), false);
      Assert.assertEquals(response.size(), replicasToReplicate.get(dataNodeId).size());
      for (int i = 0; i < response.size(); i++) {
        Assert.assertEquals(response.get(i).missingStoreKeys.size(), 3);
        Assert.assertEquals(((MockFindToken) response.get(i).remoteToken).getIndex(), 14);
      }
      replicaThread.fixMissingStoreKeys(new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
          replicaBuffers.get("localhost" + 64423), 4), replicasToReplicate.get(dataNodeId), false, response);
      for (int i = 0; i < response.size(); i++) {
        replicasToReplicate.get(dataNodeId).get(i).setToken(response.get(i).remoteToken);
      }

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, replicaStores.get("localhost" + 64423),
              replicaBuffers.get("localhost" + 64423), 4), replicasToReplicate.get(dataNodeId), false);
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
}
