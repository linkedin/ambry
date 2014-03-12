package com.github.ambry.replication;


import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.*;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.Send;
import com.github.ambry.shared.*;
import com.github.ambry.store.*;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Scheduler;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationTest {

  class MockMessageReadSet implements MessageReadSet {

    List<ByteBuffer> bytebuffers;

    public MockMessageReadSet(List<ByteBuffer> bytebuffers) {
      this.bytebuffers = bytebuffers;
    }

    @Override
    public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
      ByteBuffer bufferToWrite = bytebuffers.get(0);
      bufferToWrite.position((int)relativeOffset);
      bufferToWrite.limit((int)Math.min(maxSize, bufferToWrite.capacity()));
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
      public long appendFrom(ReadableByteChannel channel, long size) throws IOException {
        int tempIndex = index;
        index++;
        return channel.read(buflist.get(tempIndex));
      }
    }

    DummyLog log;
    List<MessageInfo> messageInfoList;
    int maxEntriesToFind;

    class DummyLog {
      private List<ByteBuffer> logInfo;
      public DummyLog(List<ByteBuffer> bufferList) {
        this.logInfo = bufferList;
      }

      public void appendData(ByteBuffer buffer) {
        logInfo.add(buffer);
      }

      public ByteBuffer getData(int index) {
        return logInfo.get(index);
      }
    }
    public MockStore(List<MessageInfo> messageInfo, List<ByteBuffer> buffers, int maxEntriesToFind) {
      if (messageInfo.size() != buffers.size())
        throw new IllegalArgumentException("message info size and buffer size does not match");
      messageInfoList = messageInfo;
      log = new DummyLog(buffers);
      this.maxEntriesToFind = maxEntriesToFind;
    }

    @Override
    public void start() throws StoreException {

    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids) throws StoreException {
      List<MessageInfo> infoOutput = new ArrayList<MessageInfo>();
      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      int index = 0;
      for (MessageInfo info : messageInfoList) {
        for (StoreKey id : ids) {
          if (info.getStoreKey().equals(id)) {
            infoOutput.add(info);
            buffers.add(log.getData(index));
          }
        }
        index++;
      }
      return new StoreInfo(new MockMessageReadSet(buffers), messageInfoList);
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {
      List<MessageInfo> messageInfoListTemp = messageSetToWrite.getMessageSetInfo();
      List<ByteBuffer> buffersToWrite = new ArrayList<ByteBuffer>();
      for (MessageInfo messageInfo : messageInfoListTemp) {
        ByteBuffer buf = ByteBuffer.allocate((int)messageInfo.getSize());
        buffersToWrite.add(buf);
      }
      try {
        messageSetToWrite.writeTo(new ByteBufferWrite(buffersToWrite));
      }
      catch (IOException e) {

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
      messageInfoList.set(index, new MessageInfo(messageInfoFound.getStoreKey(),
              messageInfoFound.getSize(),
              true,
              messageInfoFound.getTimeToLiveInMs()));
    }

    @Override
    public void updateTTL(MessageWriteSet messageSetToUpdateTTL) throws StoreException {
      int index = 0;
      MessageInfo messageInfoFound = null;
      for (MessageInfo messageInfo : messageInfoList) {
        if (messageInfo.getStoreKey().equals(messageSetToUpdateTTL.getMessageSetInfo().get(0).getStoreKey())) {
          messageInfoFound = messageInfo;
          break;
        }
        index++;
      }
      messageInfoList.set(index, new MessageInfo(messageInfoFound.getStoreKey(),
              messageInfoFound.getSize(),
              -1));
    }

    @Override
    public FindInfo findEntriesSince(FindToken token) throws StoreException {
      MockFindToken tokenmock = (MockFindToken)token;
      List<MessageInfo> entriesToReturn = new ArrayList<MessageInfo>();
      for (int i = 0; i < Math.min(maxEntriesToFind, messageInfoList.size()); i++) {
        entriesToReturn.add(messageInfoList.get(tokenmock.getIndex() + i));
      }
      return new FindInfo(entriesToReturn, new MockFindToken(tokenmock.getIndex() + entriesToReturn.size()));
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
      Set<StoreKey> keysMissing= new HashSet<StoreKey>();
      for (StoreKey key : keys) {
        boolean found = false;
        for (MessageInfo messageInfo : messageInfoList) {
          if (messageInfo.getStoreKey().equals(key)) {
            found = true;
            break;
          }
        }
        if (!found)
          keysMissing.add(key);
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
    public void shutdown() throws StoreException {
    }
  }

  class MockStoreManager extends StoreManager {
    Map<PartitionId, MockStore> stores;

    public MockStoreManager(StoreConfig config,
                            Scheduler scheduler,
                            MetricRegistry registry,
                            List<ReplicaId> replicas,
                            StoreKeyFactory factory,
                            MessageStoreRecovery recovery,
                            Map<PartitionId, MockStore> stores) {
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
      public void writeTo(WritableByteChannel channel) throws IOException {
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

    List<MessageInfo> messageInfoList;
    List<ByteBuffer> bufferList;
    int indexRequested;
    List<ByteBuffer> bufferToReturn;
    List<MessageInfo> messageInfoToReturn;
    String host;
    int port;
    int maxSizeToReturn;

    public MockConnection(String host,
                          int port,
                          List<MessageInfo> messageInfoList,
                          List<ByteBuffer> bufferList,
                          int maxSizeToReturn) {
      this.messageInfoList = messageInfoList;
      this.bufferList = bufferList;
      this.host = host;
      this.port = port;
      this.maxSizeToReturn = maxSizeToReturn;
    }

    @Override
    public void send(Send request) throws IOException {
      if (request instanceof ReplicaMetadataRequest) {
        ReplicaMetadataRequest metadataRequest = (ReplicaMetadataRequest)request;
        MockFindToken token = (MockFindToken)metadataRequest.getToken();
        indexRequested = token.getIndex();
      }
      if (request instanceof GetRequest) {
        indexRequested = -1;
        GetRequest getRequest = (GetRequest)request;
        bufferToReturn = new ArrayList<ByteBuffer>();
        messageInfoToReturn = new ArrayList<MessageInfo>();
        for (StoreKey key : getRequest.getBlobIds()) {
          int index = 0;
          for (MessageInfo info : messageInfoList) {
            if (key.equals(info.getStoreKey())) {
              messageInfoToReturn.add(info);
              bufferToReturn.add(bufferList.get(index));
            }
            index++;
          }
        }
      }
    }

    @Override
    public InputStream receive() throws IOException {
      Response response = null;
      if (indexRequested != -1) {
        List<MessageInfo> messageInfoToReturn = new ArrayList<MessageInfo>();
        int startIndex = indexRequested;
        for (int i = startIndex; i < startIndex + maxSizeToReturn; i++) {
          messageInfoToReturn.add(messageInfoList.get(i));
          indexRequested = i;
        }
        response = new ReplicaMetadataResponse(1,
                                               "replicametadata",
                                               ServerErrorCode.No_Error,
                                               new MockFindToken(indexRequested),
                                               messageInfoToReturn);
        indexRequested = -1;
      }
      else {
        response = new GetResponse(1,
                                   "replication",
                                   messageInfoToReturn,
                                   new MockSend(bufferToReturn),
                                   ServerErrorCode.No_Error);

      }
      ByteBuffer buffer = ByteBuffer.allocate((int)response.sizeInBytes());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer);
      WritableByteChannel channel = Channels.newChannel(stream);
      while (!response.isSendComplete()) {
        response.writeTo(channel);
      }
      buffer.flip();
      buffer.getLong();
      return new ByteBufferInputStream(buffer);
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

    Map<String, List<MessageInfo>> messageInfoList;
    Map<String, List<ByteBuffer>> byteBufferList;
    int maxEntriesToReturn;

    public MockConnectionPool(Map<String, List<MessageInfo>> messageInfoList,
                              Map<String, List<ByteBuffer>> byteBufferList,
                              int maxEntriesToReturn) {
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
      return new MockConnection(host,
                                port,
                                messageInfoList.get(host+port),
                                byteBufferList.get(host+port),
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
      MockClusterMap clusterMap = new MockClusterMap();
      List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
      MockDataNodeId mockDataNode = new MockDataNodeId(6667);
      for (ReplicaId replicaId : clusterMap.getReplicaIds(mockDataNode)) {
        if (replicaId.getDataNodeId().getPort() == 6667) {
          for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
            RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(peerReplicaId, new MockFindToken(0), 1000000);
            remoteReplicas.add(remoteReplicaInfo);
          }
        }
      }
      List<MessageInfo> messageInfoListLocalReplica = new ArrayList<MessageInfo>();
      List<ByteBuffer> messageBufferListLocalReplica = new ArrayList<ByteBuffer>();

      List<MessageInfo> messageInfoListRemoteReplica2 = new ArrayList<MessageInfo>();
      List<ByteBuffer> messageBufferListLocalReplica2 = new ArrayList<ByteBuffer>();

      List<MessageInfo> messageInfoListRemoteReplica3 = new ArrayList<MessageInfo>();
      List<ByteBuffer> messageBufferListLocalReplica3 = new ArrayList<ByteBuffer>();

      for (int i = 0; i < 10; i++) {
        BlobId id = new BlobId(remoteReplicas.get(0).getReplicaId().getPartitionId());
        messageInfoListLocalReplica.add(new MessageInfo(id, 1000));
        messageInfoListRemoteReplica2.add(new MessageInfo(id, 1000));
        messageInfoListRemoteReplica3.add(new MessageInfo(id, 1000));
        byte[] bytes = new byte[1000];
        new Random().nextBytes(bytes);
        messageBufferListLocalReplica.add(ByteBuffer.wrap(bytes));
        messageBufferListLocalReplica2.add(ByteBuffer.wrap(bytes));
        messageBufferListLocalReplica3.add(ByteBuffer.wrap(bytes));
      }

      // add additional messages to replica 2 and replica 3
      for (int i = 10; i < 15; i++) {
        BlobId id = new BlobId(remoteReplicas.get(0).getReplicaId().getPartitionId());
        messageInfoListRemoteReplica2.add(new MessageInfo(id, 1000));
        messageInfoListRemoteReplica3.add(new MessageInfo(id, 1000));
        byte[] bytes = new byte[1000];
        new Random().nextBytes(bytes);
        messageBufferListLocalReplica2.add(ByteBuffer.wrap(bytes));
        messageBufferListLocalReplica3.add(ByteBuffer.wrap(bytes));
      }

      // add additional messages to replica 3
      for (int i = 15; i < 18; i++) {
        BlobId id = new BlobId(remoteReplicas.get(0).getReplicaId().getPartitionId());
        messageInfoListRemoteReplica3.add(new MessageInfo(id, 1000));
        byte[] bytes = new byte[1000];
        new Random().nextBytes(bytes);
        messageBufferListLocalReplica3.add(ByteBuffer.wrap(bytes));
      }

      PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas,
                                                      remoteReplicas.get(0).getReplicaId().getPartitionId(),
                                                      new MockStore(messageInfoListLocalReplica,
                                                                    messageBufferListLocalReplica,
                                                                    4));
      ArrayList<PartitionInfo> partitionInfoList = new ArrayList<PartitionInfo>();
      partitionInfoList.add(partitionInfo);
      Map<String, List<MessageInfo>> replicaStores = new HashMap<String, List<MessageInfo>>();
      replicaStores.put("localhost"+6668, messageInfoListRemoteReplica2);
      replicaStores.put("localhost"+6669, messageInfoListRemoteReplica3);

      Map<String, List<ByteBuffer>> replicaBuffers = new HashMap<String, List<ByteBuffer>>();
      replicaBuffers.put("localhost"+6668, messageBufferListLocalReplica2);
      replicaBuffers.put("localhost"+6669, messageBufferListLocalReplica3);

      ReplicaThread replicaThread = new ReplicaThread("threadtest",
                                                      partitionInfoList,
                                                      new MockFindTokenFactory(),
                                                      clusterMap,
                                                      new AtomicInteger(0),
                                                      mockDataNode,
                                                      new MockConnectionPool(replicaStores, replicaBuffers, 3),
                                                      1000);
      ReplicaThread.ExchangeMetadataResponse response =
              replicaThread.exchangeMetadata(new MockConnection("localhost",
                                                                6668,
                                                                messageInfoListRemoteReplica2,
                                                                messageBufferListLocalReplica2,
                                                                5),
                                             partitionInfo,
                                             remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 0);
      Assert.assertEquals(((MockFindToken)response.remoteToken).getIndex(), 4);
      remoteReplicas.get(1).setToken(response.remoteToken);

      response = replicaThread.exchangeMetadata(new MockConnection("localhost",
                                                                   6668,
                                                                   messageInfoListRemoteReplica2,
                                                                   messageBufferListLocalReplica2,
                                                                   5),
                                                partitionInfo,
                                                remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 0);
      Assert.assertEquals(((MockFindToken)response.remoteToken).getIndex(), 8);
      remoteReplicas.get(1).setToken(response.remoteToken);

      response = replicaThread.exchangeMetadata(new MockConnection("localhost",
                                                                   6668,
                                                                   messageInfoListRemoteReplica2,
                                                                   messageBufferListLocalReplica2,
                                                                   4),
                                                partitionInfo,
                                                remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 2);
      Assert.assertEquals(((MockFindToken)response.remoteToken).getIndex(), 11);
      replicaThread.fixMissingStoreKeys(response.missingStoreKeys,
                                        partitionInfo,
                                        new MockConnection("localhost",
                                                           6668,
                                                           messageInfoListRemoteReplica2,
                                                           messageBufferListLocalReplica2,
                                                           4));
      remoteReplicas.get(1).setToken(response.remoteToken);
      response = replicaThread.exchangeMetadata(new MockConnection("localhost",
                                                                   6668,
                                                                   messageInfoListRemoteReplica2,
                                                                   messageBufferListLocalReplica2,
                                                                   4),
                                                partitionInfo,
                                                remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 3);
      Assert.assertEquals(((MockFindToken)response.remoteToken).getIndex(), 14);
      replicaThread.fixMissingStoreKeys(response.missingStoreKeys,
                                        partitionInfo,
                                        new MockConnection("localhost",
                                                6668,
                                                messageInfoListRemoteReplica2,
                                                messageBufferListLocalReplica2,
                                                4));
      //check replica1 store is the same as replica 2 store in messageinfo and byte buffers
      for (MessageInfo messageInfo : messageInfoListLocalReplica) {
        boolean found = false;
        for (MessageInfo messageInfo1 : messageInfoListRemoteReplica2) {
          if (messageInfo.getStoreKey().equals(messageInfo1.getStoreKey()))
            found = true;
        }
        Assert.assertTrue(found);
      }
      for (ByteBuffer buf : messageBufferListLocalReplica) {
        boolean found = false;
        for (ByteBuffer bufActual : messageBufferListLocalReplica2) {
          if (Arrays.equals(buf.array(), bufActual.array()))
            found = true;
        }
        Assert.assertTrue(found);
      }

    }
    catch (Exception e) {
       Assert.assertTrue(false);
    }
  }
}
