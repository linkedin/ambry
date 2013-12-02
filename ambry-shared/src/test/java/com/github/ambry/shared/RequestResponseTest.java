package com.github.ambry.shared;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.ByteBufferInputStream;
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

// TODO
public class RequestResponseTest {

  @Test
  public void putRequestSerDe() throws IOException {
    Random rnd = new Random();

    int correlationId = 5;
    String clientId = "client";
    BlobId blobId = new BlobId(new MockPartitionId());
    byte []userMetadata = new byte[50];
    rnd.nextBytes(userMetadata);
    ByteBuffer.wrap(userMetadata);

    int dataSize = 100;
    byte []data = new byte[dataSize];
    rnd.nextBytes(data);


    BlobProperties blobProperties = new BlobProperties(BlobProperties.Infinite_TTL,
                                                       false,
                                                       "contentType",
                                                       "memberId",
                                                       "parentBlobId",
                                                       dataSize,
                                                       "serviceID");

    PutRequest request = new PutRequest(correlationId,
                                        clientId,
                                        blobId,
                                        blobProperties, ByteBuffer.wrap(userMetadata),
                                        new ByteBufferInputStream(ByteBuffer.wrap(data))
    );

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

}

/**
 * Mock partition id for unit tests
 */
class MockPartitionId extends PartitionId {
  private Long partition = 88L;

  public MockPartitionId() {
  }

  @Override
  public byte[] getBytes() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(partition);
    return buf.array();
  }

  @Override
  public List<ReplicaId> getReplicaIds() {
    return null;
  }

  @Override
  public PartitionState getPartitionState() {
    return PartitionState.READ_WRITE;
  }

  @Override
  public int compareTo(PartitionId o) {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MockPartitionId mockPartition = (MockPartitionId)o;

    if (partition != mockPartition.partition) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int)(partition ^ (partition >>> 32));
  }

  @Override
  public String toString() {
    return partition.toString();
  }
}
