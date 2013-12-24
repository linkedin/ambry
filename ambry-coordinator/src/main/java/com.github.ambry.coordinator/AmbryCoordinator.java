package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.DeleteRequest;
import com.github.ambry.shared.DeleteResponse;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.shared.TTLRequest;
import com.github.ambry.shared.TTLResponse;
import java.io.IOException;

import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

class ConnectionPool {
  String host;
  int port;
  Object lock;
  Queue<BlockingChannel> channel = new LinkedList<BlockingChannel>();

  public ConnectionPool(String host, int port) {
    this.host = host;
    this.port = port;
    this.lock = new Object();
  }

  public BlockingChannel getConnection() throws IOException {
    synchronized (lock) {
      if (channel.size() > 0)
        return channel.remove();
      else {
        BlockingChannel resource = new BlockingChannel(host, port, 10000, 10000, 10000);
        resource.connect();
        return resource;
      }
    }
  }

  public void returnConnection(BlockingChannel connection) {
    synchronized (lock) {
      channel.add(connection);
    }
  }

  public void clean() {
    for (BlockingChannel ch : channel) {
      ch.disconnect();
    }
  }
}

public class AmbryCoordinator implements Coordinator {

  ClusterMap map;
  Random r = new Random();
  HashMap<String, ConnectionPool> pool;

  public AmbryCoordinator( ClusterMap map) {
    this.map = map;
    pool = new HashMap<String, ConnectionPool>();
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blob) {
    try {
      // put blob
      PartitionId partition = getRandomPartition();
      System.out.println("here1");
      DataNodeId node = partition.getReplicaIds().get(0).getDataNodeId();
      ConnectionPool resource = pool.get(node.getHostname() + node.getPort());
      if (resource == null) {
        resource = new ConnectionPool(node.getHostname(), node.getPort());
        pool.put(node.getHostname() + node.getPort(), resource);
      }
      BlockingChannel channel = resource.getConnection();
      PutRequest putRequest = new PutRequest(1, "client1", new BlobId(partition), userMetadata, blob, blobProperties);
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      resource.returnConnection(channel);
      return putRequest.getBlobId().toString();
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
      System.out.println("error " + e);
    }
    return null; // this will not happen in the actual implementation
  }

  @Override
  public void deleteBlob(String id) throws BlobNotFoundException {
    try {
      // delete blob
      BlobId blobId = new BlobId(id, map);
      DeleteRequest deleteRequest = new DeleteRequest(1, "client1", blobId);
      DataNodeId node = blobId.getPartition().getReplicaIds().get(0).getDataNodeId();
      ConnectionPool resource = pool.get(node.getHostname() + node.getPort());
      if (resource == null) {
        resource = new ConnectionPool(node.getHostname(), node.getPort());
        pool.put(node.getHostname() + node.getPort(), resource);
      }
      BlockingChannel channel = resource.getConnection();
      channel.send(deleteRequest);
      InputStream deleteResponseStream = channel.receive();
      DeleteResponse response = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
      resource.returnConnection(channel);
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
  }

  @Override
  public void updateTTL(String id, long newTTL) throws BlobNotFoundException {
    try {
      // update ttl of the blob
      BlobId blobId = new BlobId(id, map);
      TTLRequest ttlRequest = new TTLRequest(1, "client1", blobId, newTTL);
      DataNodeId node = blobId.getPartition().getReplicaIds().get(0).getDataNodeId();
      ConnectionPool resource = pool.get(node.getHostname() + node.getPort());
      if (resource == null) {
        resource = new ConnectionPool(node.getHostname(), node.getPort());
        pool.put(node.getHostname() + node.getPort(), resource);
      }
      BlockingChannel channel = resource.getConnection();
      channel.send(ttlRequest);
      InputStream ttlResponseStream = channel.receive();
      TTLResponse response = TTLResponse.readFrom(new DataInputStream(ttlResponseStream));
      resource.returnConnection(channel);
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
  }

  @Override
  public BlobOutput getBlob(String blobId) throws BlobNotFoundException {
    // get blob
    try {
      BlobId id = new BlobId(blobId, map);
      DataNodeId node = id.getPartition().getReplicaIds().get(0).getDataNodeId();
      ConnectionPool resource = pool.get(node.getHostname() + node.getPort());
      if (resource == null) {
        resource = new ConnectionPool(node.getHostname(), node.getPort());
        pool.put(node.getHostname() + node.getPort(), resource);
      }
      BlockingChannel channel = resource.getConnection();
      GetResponse response = doGetResponse(id, MessageFormatFlags.Data, channel);
      BlobOutput output = MessageFormat.deserializeData(response.getInputStream());
      resource.returnConnection(channel);
      return output;
    }
    catch (Exception e) {
      System.out.println("error " + e);
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
    return null; // this will never happen once Ambry Exception is defined
  }

  @Override
  public ByteBuffer getUserMetadata(String blobId) throws BlobNotFoundException {
    try {
      BlobId id = new BlobId(blobId, map);
      DataNodeId node = id.getPartition().getReplicaIds().get(0).getDataNodeId();
      ConnectionPool resource = pool.get(node.getHostname() + node.getPort());
      if (resource == null) {
        resource = new ConnectionPool(node.getHostname(), node.getPort());
        pool.put(node.getHostname() + node.getPort(), resource);
      }
      BlockingChannel channel = resource.getConnection();
      GetResponse response = doGetResponse(id, MessageFormatFlags.UserMetadata, channel);
      ByteBuffer userMetadata = MessageFormat.deserializeMetadata(response.getInputStream());
      resource.returnConnection(channel);
      return userMetadata;
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
    return null; // this will never happen once Ambry Exception is defined
  }

  @Override
  public BlobProperties getBlobProperties(String blobId) throws BlobNotFoundException {
    try {
      BlobId id = new BlobId(blobId, map);
      DataNodeId node = id.getPartition().getReplicaIds().get(0).getDataNodeId();
      ConnectionPool resource = pool.get(node.getHostname() + node.getPort());
      if (resource == null) {
        resource = new ConnectionPool(node.getHostname(), node.getPort());
        pool.put(node.getHostname() + node.getPort(), resource);
      }
      BlockingChannel channel = resource.getConnection();
      GetResponse response = doGetResponse(id, MessageFormatFlags.BlobProperties, channel);
      BlobProperties properties = MessageFormat.deserializeBlobProperties(response.getInputStream());
      resource.returnConnection(channel);
      return properties;
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
    return null; // this will never happen once Ambry Exception is defined
  }

  private GetResponse doGetResponse(BlobId blobId, MessageFormatFlags flag, BlockingChannel channel) {
    try {
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      ids.add(blobId);
      GetRequest getRequest = new GetRequest(blobId.getPartition(), 1, "clientid2", flag, ids);
      channel.send(getRequest);
      InputStream stream = channel.receive();
      GetResponse response = GetResponse.readFrom(new DataInputStream(stream), map);
      return response;
    }
    catch (Exception e) {
      System.out.println("error " + e);
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
    return null; // this will never happen once Ambry Exception is defined
  }

  public void shutdown() {
    for (Map.Entry<String, ConnectionPool> entry : pool.entrySet()) {
      entry.getValue().clean();
    }
  }

  // Temporary method to generate random partition
  private PartitionId getRandomPartition() {
    long count = map.getWritablePartitionIdsCount();
    int index = r.nextInt((int)count);
    return map.getWritablePartitionIdAt(index);
  }
}
