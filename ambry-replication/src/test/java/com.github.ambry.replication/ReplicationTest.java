package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Send;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.ConnectedChannel;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.ConnectionPoolTimeoutException;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.ReplicaMetadataRequest;
import com.github.ambry.shared.ReplicaMetadataResponse;
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
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreManager;
import com.github.ambry.store.Write;
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
      public long appendFrom(ReadableByteChannel channel, long size)
          throws IOException {
        int tempIndex = index;
        index++;
        return channel.read(buflist.get(tempIndex));
      }
    }

    DummyLog log;
    List<MessageInfo> messageInfoList;
    long bytesWrittenSoFar;

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
      bytesWrittenSoFar = new Long(0);
    }

    @Override
    public void start()
        throws StoreException {

    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids)
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
    public void updateTTL(MessageWriteSet messageSetToUpdateTTL)
        throws StoreException {
      int index = 0;
      MessageInfo messageInfoFound = null;
      for (MessageInfo messageInfo : messageInfoList) {
        if (messageInfo.getStoreKey().equals(messageSetToUpdateTTL.getMessageSetInfo().get(0).getStoreKey())) {
          messageInfoFound = messageInfo;
          break;
        }
        index++;
      }
      messageInfoList.set(index, new MessageInfo(messageInfoFound.getStoreKey(), messageInfoFound.getSize(), -1));
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxSizeOfEntries)
        throws StoreException {
      MockFindToken tokenmock = (MockFindToken) token;
      List<MessageInfo> entriesToReturn = new ArrayList<MessageInfo>();
      long currentSizeOfEntries = 0;
      int index = 0;
      while (currentSizeOfEntries < maxSizeOfEntries && index < messageInfoList.size()) {
        entriesToReturn.add(messageInfoList.get(tokenmock.getIndex() + index));
        index++;
      }
      // last parameter for FindInfo is not accurate. Ignoring for now, as it is a testcase.
      return new FindInfo(entriesToReturn, new MockFindToken(tokenmock.getIndex() + entriesToReturn.size()),
           entriesToReturn.size());
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

    List<MessageInfo> messageInfoList;
    List<ByteBuffer> bufferList;
    int indexRequested;
    List<ByteBuffer> bufferToReturn;
    List<MessageInfo> messageInfoToReturn;
    String host;
    int port;
    int maxSizeToReturn;

    public MockConnection(String host, int port, List<MessageInfo> messageInfoList, List<ByteBuffer> bufferList,
        int maxSizeToReturn) {
      this.messageInfoList = messageInfoList;
      this.bufferList = bufferList;
      this.host = host;
      this.port = port;
      this.maxSizeToReturn = maxSizeToReturn;
    }

    @Override
    public void send(Send request)
        throws IOException {
      if (request instanceof ReplicaMetadataRequest) {
        ReplicaMetadataRequest metadataRequest = (ReplicaMetadataRequest) request;
        MockFindToken token = (MockFindToken) metadataRequest.getToken();
        indexRequested = token.getIndex();
      }
      if (request instanceof GetRequest) {
        indexRequested = -1;
        GetRequest getRequest = (GetRequest) request;
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
    public InputStream receive()
        throws IOException {
      Response response = null;
      if (indexRequested != -1) {
        List<MessageInfo> messageInfoToReturn = new ArrayList<MessageInfo>();
        int startIndex = indexRequested;
        for (int i = startIndex; i < startIndex + maxSizeToReturn; i++) {
          messageInfoToReturn.add(messageInfoList.get(i));
          indexRequested = i;
        }
        response = new ReplicaMetadataResponse(1, "replicametadata", ServerErrorCode.No_Error,
            new MockFindToken(indexRequested), messageInfoToReturn);
        indexRequested = -1;
      } else {
        response = new GetResponse(1, "replication", messageInfoToReturn, new MockSend(bufferToReturn),
            ServerErrorCode.No_Error);
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) response.sizeInBytes());
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
        Map<String, List<ByteBuffer>> byteBufferList, int maxEntriesToReturn) {
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
      List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>();
      for (ReplicaId replicaId : clusterMap.getReplicaIds(clusterMap.getDataNodeId("localhost", 64422))) {
        if (replicaId.getDataNodeId().getPort() == 64422) {
          for (ReplicaId peerReplicaId : replicaId.getPeerReplicaIds()) {
            RemoteReplicaInfo remoteReplicaInfo =
                new RemoteReplicaInfo(peerReplicaId, null, new MockFindToken(0), 1000000);
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

      PartitionInfo partitionInfo =
          new PartitionInfo(remoteReplicas, remoteReplicas.get(0).getReplicaId().getPartitionId(),
              new MockStore(messageInfoListLocalReplica, messageBufferListLocalReplica));
      ArrayList<PartitionInfo> partitionInfoList = new ArrayList<PartitionInfo>();
      partitionInfoList.add(partitionInfo);
      Map<String, List<MessageInfo>> replicaStores = new HashMap<String, List<MessageInfo>>();
      replicaStores.put("localhost" + 64423, messageInfoListRemoteReplica2);
      replicaStores.put("localhost" + 64424, messageInfoListRemoteReplica3);

      Map<String, List<ByteBuffer>> replicaBuffers = new HashMap<String, List<ByteBuffer>>();
      replicaBuffers.put("localhost" + 64423, messageBufferListLocalReplica2);
      replicaBuffers.put("localhost" + 64424, messageBufferListLocalReplica3);
      ReplicationConfig config = new ReplicationConfig(new VerifiableProperties(new Properties()));

      ReplicationMetrics replicationMetrics =
          new ReplicationMetrics("replication", new MetricRegistry(), new ArrayList<ReplicaThread>());
      ReplicaThread replicaThread =
          new ReplicaThread("threadtest", partitionInfoList, new MockFindTokenFactory(), clusterMap,
              new AtomicInteger(0), clusterMap.getDataNodeId("localhost", 64422),
              new MockConnectionPool(replicaStores, replicaBuffers, 3), config, replicationMetrics, null);
      ReplicaThread.ExchangeMetadataResponse response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, messageInfoListRemoteReplica2, messageBufferListLocalReplica2, 5),
          partitionInfo, remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 0);
      Assert.assertEquals(((MockFindToken) response.remoteToken).getIndex(), 4);
      remoteReplicas.get(1).setToken(response.remoteToken);

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, messageInfoListRemoteReplica2, messageBufferListLocalReplica2, 5),
          partitionInfo, remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 0);
      Assert.assertEquals(((MockFindToken) response.remoteToken).getIndex(), 8);
      remoteReplicas.get(1).setToken(response.remoteToken);

      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, messageInfoListRemoteReplica2, messageBufferListLocalReplica2, 4),
          partitionInfo, remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 2);
      Assert.assertEquals(((MockFindToken) response.remoteToken).getIndex(), 11);
      replicaThread.fixMissingStoreKeys(response.missingStoreKeys, partitionInfo,
          new MockConnection("localhost", 64423, messageInfoListRemoteReplica2, messageBufferListLocalReplica2, 4),
          false);
      remoteReplicas.get(1).setToken(response.remoteToken);
      response = replicaThread.exchangeMetadata(
          new MockConnection("localhost", 64423, messageInfoListRemoteReplica2, messageBufferListLocalReplica2, 4),
          partitionInfo, remoteReplicas.get(1));
      Assert.assertEquals(response.missingStoreKeys.size(), 3);
      Assert.assertEquals(((MockFindToken) response.remoteToken).getIndex(), 14);
      replicaThread.fixMissingStoreKeys(response.missingStoreKeys, partitionInfo,
          new MockConnection("localhost", 64423, messageInfoListRemoteReplica2, messageBufferListLocalReplica2, 4),
          false);
      //check replica1 store is the same as replica 2 store in messageinfo and byte buffers
      for (MessageInfo messageInfo : messageInfoListLocalReplica) {
        boolean found = false;
        for (MessageInfo messageInfo1 : messageInfoListRemoteReplica2) {
          if (messageInfo.getStoreKey().equals(messageInfo1.getStoreKey())) {
            found = true;
          }
        }
        Assert.assertTrue(found);
      }
      for (ByteBuffer buf : messageBufferListLocalReplica) {
        boolean found = false;
        for (ByteBuffer bufActual : messageBufferListLocalReplica2) {
          if (Arrays.equals(buf.array(), bufActual.array())) {
            found = true;
          }
        }
        Assert.assertTrue(found);
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }
}
