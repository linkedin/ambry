package com.github.ambry.shared;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Random;


class MockFindTokenFactory implements FindTokenFactory {

  @Override
  public FindToken getFindToken(DataInputStream stream)
      throws IOException {
    return new MockFindToken(stream);
  }

  @Override
  public FindToken getNewFindToken() {
    return new MockFindToken(0, 0);
  }
}

class MockFindToken implements FindToken {
  int index;
  long bytesRead;

  public MockFindToken(int index, long bytesRead) {
    this.index = index;
    this.bytesRead = bytesRead;
  }

  public MockFindToken(DataInputStream stream)
      throws IOException {
    this.index = stream.readInt();
    this.bytesRead = stream.readLong();
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(12);
    byteBuffer.putInt(index);
    byteBuffer.putLong(bytesRead);
    return byteBuffer.array();
  }

  public int getIndex() {
    return index;
  }

  public long getBytesRead() {
    return this.bytesRead;
  }
}

// TODO
public class RequestResponseTest {

  @Test
  public void putRequestSerDe()
      throws IOException {
    Random rnd = new Random();

    int correlationId = 5;
    String clientId = "client";
    BlobId blobId = new BlobId(new MockPartitionId());
    byte[] userMetadata = new byte[50];
    rnd.nextBytes(userMetadata);
    ByteBuffer.wrap(userMetadata);

    int dataSize = 100;
    byte[] data = new byte[dataSize];
    rnd.nextBytes(data);

    BlobProperties blobProperties =
        new BlobProperties(dataSize, "serviceID", "memberId", "contentType", false, Utils.Infinite_Time);

    PutRequest request = new PutRequest(correlationId, clientId, blobId, blobProperties, ByteBuffer.wrap(userMetadata),
        new ByteBufferInputStream(ByteBuffer.wrap(data)));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
    request.writeTo(writableByteChannel);

    InputStream responseStream = new ByteArrayInputStream(outputStream.toByteArray());
    // TODO: Need ClusterMap to do this Deserialization. ClusterMap is really ugly at this level.
  }

  @Test
  public void putRequestResponseTest() {
    //PutRequest request = new PutRequest(0, 0, "client", new BlobId("id1"), )
  }

  @Test
  public void getRequestResponseTest() {

  }

  @Test
  public void deleteRequestResponseTest() {

  }

  @Test
  public void ttlRequestResponseTest() {

  }

  @Test
  public void replicaMetadataRequestTest()
      throws IOException {
    List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList = new ArrayList<ReplicaMetadataRequestInfo>();
    ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
        new ReplicaMetadataRequestInfo(new MockPartitionId(), new MockFindToken(0, 1000), "localhost", "path");
    replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
    ReplicaMetadataRequest request = new ReplicaMetadataRequest(1, "id", replicaMetadataRequestInfoList, 1000);
    ByteBuffer buffer = ByteBuffer.allocate((int) request.sizeInBytes());
    ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(buffer);
    request.writeTo(Channels.newChannel(byteBufferOutputStream));
    buffer.flip();
    buffer.getLong();
    buffer.getShort();
    ReplicaMetadataRequest replicaMetadataRequestFromBytes = ReplicaMetadataRequest
        .readFrom(new DataInputStream(new ByteBufferInputStream(buffer)), new MockClusterMap(),
            new MockFindTokenFactory());
    Assert.assertEquals(replicaMetadataRequestFromBytes.getMaxTotalSizeOfEntriesInBytes(), 1000);
    Assert.assertEquals(replicaMetadataRequestFromBytes.getReplicaMetadataRequestInfoList().size(), 1);

    try {
      request = new ReplicaMetadataRequest(1, "id", null, 12);
      buffer = ByteBuffer.allocate((int) request.sizeInBytes());
      byteBufferOutputStream = new ByteBufferOutputStream(buffer);
      request.writeTo(Channels.newChannel(byteBufferOutputStream));
      Assert.assertEquals(true, false);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(true, true);
    }
    try {
      replicaMetadataRequestInfo = new ReplicaMetadataRequestInfo(new MockPartitionId(), null, "localhost", "path");
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
  }
}
