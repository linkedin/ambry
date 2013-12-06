package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormat;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.shared.*;
import com.github.ambry.store.MessageInfo;

import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.List;

public class AmbryCoordinator implements Coordinator {

  private String host;
  private int port;
  BlockingChannel channel;
  ClusterMap map;
  Random r = new Random();

  public AmbryCoordinator(String host, int port, ClusterMap map) {
    this.host = host;
    this.port = port;
    this.map = map;
    channel = new BlockingChannel(host, port, 10000, 10000, 10000);
    try {
      channel.connect();
    }
    catch (Exception e) {

    }
  }

  @Override
  public String putBlob(BlobProperties blobProperties, ByteBuffer userMetadata, InputStream blob) {
    try {
      // put blob
      String id = UUID.randomUUID().toString();
      PutRequest putRequest = new PutRequest(1, "client1", new BlobId(getRandomPartition()), userMetadata, blob, blobProperties);
      channel.send(putRequest);
      InputStream putResponseStream = channel.receive();
      PutResponse response = PutResponse.readFrom(new DataInputStream(putResponseStream));
      return id;
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
      DeleteRequest deleteRequest = new DeleteRequest(1, "client1", new BlobId(getRandomPartition()));
      BlockingChannel channel = new BlockingChannel(host, port, 10000, 10000, 10000);
      channel.connect();
      channel.send(deleteRequest);
      InputStream deleteResponseStream = channel.receive();
      DeleteResponse response = DeleteResponse.readFrom(new DataInputStream(deleteResponseStream));
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
  }

  @Override
  public void updateTTL(String id, long newTTL) throws BlobNotFoundException {
    try {
      // update ttl of the blob
      TTLRequest ttlRequest = new TTLRequest(1, "client1", new BlobId(getRandomPartition()), newTTL);
      BlockingChannel channel = new BlockingChannel(host, port, 10000, 10000, 10000);
      channel.connect();
      channel.send(ttlRequest);
      InputStream ttlResponseStream = channel.receive();
      TTLResponse response = TTLResponse.readFrom(new DataInputStream(ttlResponseStream));
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
  }

  @Override
  public BlobOutput getBlob(String blobId) throws BlobNotFoundException {
    // get blob
    try {
      GetResponse response = doGetResponse(blobId, MessageFormatFlags.Data, channel);
      return MessageFormat.deserializeData(channel.readChannel);
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
      BlockingChannel channel = new BlockingChannel(host, port, 10000, 10000, 10000);
      channel.connect();
      GetResponse response = doGetResponse(blobId, MessageFormatFlags.UserMetadata, channel);
      ByteBuffer userMetadata = MessageFormat.deserializeMetadata(response.getInputStream());
      channel.disconnect();
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
      BlockingChannel channel = new BlockingChannel(host, port, 10000, 10000, 10000);
      channel.connect();
      GetResponse response = doGetResponse(blobId, MessageFormatFlags.BlobProperties, channel);
      BlobProperties properties = MessageFormat.deserializeBlobProperties(response.getInputStream());
      channel.disconnect();
      return properties;
    }
    catch (Exception e) {
      // need to retry on errors by choosing another partition. If it still fails, throw AmbryException
    }
    return null; // this will never happen once Ambry Exception is defined
  }

  private GetResponse doGetResponse(String blobId, MessageFormatFlags flag, BlockingChannel channel) {
    try {
      ArrayList<BlobId> ids = new ArrayList<BlobId>();
      PartitionId partitionId = getRandomPartition();
      ids.add(new BlobId(partitionId));
      GetRequest getRequest = new GetRequest(partitionId, 1, "clientid2", flag, ids);
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
    if (channel != null) {
      channel.disconnect();
    }
  }

  // Temporary method to generate random partition
  private PartitionId getRandomPartition() {
    long count = map.getWritablePartitionIdsCount();
    int index = r.nextInt((int)count);
    return map.getWritablePartitionIdAt(index);
  }
}