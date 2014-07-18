package com.github.ambry.shared;

import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
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
}
