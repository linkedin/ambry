/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.RouterTestHelpers.*;


/**
 * Class to test the {@link NonBlockingRouter} for S3.
 */
@RunWith(Parameterized.class)
public class S3NonBlockingRouterTest extends NonBlockingRouterTestBase {
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String S3_PREFIX = "/s3";
  private static final String SLASH = "/";
  private String accountName = "myAccount";
  private String containerName = "container-a";
  private String blobName = "MyDirectory/MyKey";
  private boolean routerEnableReservedMetadata;

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @param routerEnableReservedMetadata  if true, enable router reserved metadata chunk feature.
   * @throws Exception if initialization fails
   */
  public S3NonBlockingRouterTest(boolean testEncryption, boolean routerEnableReservedMetadata)
      throws Exception {
    super(testEncryption, MessageFormatRecord.Metadata_Content_Version_V3, false);
    this.routerEnableReservedMetadata = routerEnableReservedMetadata;
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}});
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link QuotaAwareOperationController}.
   * @return the created VerifiableProperties instance.
   */
  @Override
  protected Properties getNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties =
        super.getNonBlockingRouterProperties(routerDataCenter, PUT_REQUEST_PARALLELISM, DELETE_REQUEST_PARALLELISM);
    properties.setProperty("router.operation.controller", "com.github.ambry.router.QuotaAwareOperationController");
    properties.setProperty("router.reserved.metadata.enabled", routerEnableReservedMetadata ? "true" : "false");

    return properties;
  }

  /**
   * Test the S3 regular PutObject. It returns the Ambry blob id.
   * No matter if router enables "reserved metadata chunk" feature, S3 regular upload doesn't reserve metadata chunk.
   * @throws Exception
   */
  @Test
  public void testS3RegularPutObject() throws Exception {
    int chunkNumber = 9;
    // composite blob with "chunkNumber" of data chunks
    maxPutChunkSize = PUT_CONTENT_SIZE / chunkNumber;
    if (PUT_CONTENT_SIZE % maxPutChunkSize != 0) {
      maxPutChunkSize++;
    }
    setRouter();
    setOperationParams();

    // regular PutObject
    RestRequest request = createRestRequestForPutOperation();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(request, null, CLUSTER_NAME));
    PutBlobOptions putBlobOptions = new PutBlobOptionsBuilder().restRequest(request).build();
    String blobID = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putBlobOptions)
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // Get Data Chunk List
    GetBlobResult getChunkResult = router.getBlob(blobID,
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobChunkIds).build(), null, null).get();
    List<StoreKey> chunkIds = getChunkResult.getBlobChunkIds();

    // verify the content, one data chunk after another
    int chunkSize = maxPutChunkSize;
    for (int i = 0; i < chunkIds.size(); i++) {
      BlobId chunkId = (BlobId) chunkIds.get(i);
      GetBlobResult chunkResult =
          router.getBlob(chunkId.getID(), new GetBlobOptionsBuilder().build(), null, null).get();
      // last chunk
      if (i == chunkNumber - 1) {
        chunkSize = PUT_CONTENT_SIZE - maxPutChunkSize * i;
      }

      // verify the reserved metadata chunk id is null
      BlobProperties blobProperties = chunkResult.getBlobInfo().getBlobProperties();
      Assert.assertEquals(blobProperties.getReservedMetadataBlobId(), null);

      // verify data content
      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      chunkResult.getBlobDataChannel().readInto(retainingAsyncWritableChannel, null).get();
      InputStream input = retainingAsyncWritableChannel.consumeContentAsInputStream();
      retainingAsyncWritableChannel.close();
      Assert.assertEquals(chunkSize, input.available());
      byte[] readContent = Utils.readBytesFromStream(input, chunkSize);
      input.close();

      byte[] originSlice = Arrays.copyOfRange(putContent, i * maxPutChunkSize, i * maxPutChunkSize + chunkSize);
      Assert.assertArrayEquals(originSlice, readContent);
    }
  }

  /**
   * Test the PutBlob with skipCompositeChunk option. It's used by S3 multipart upload.
   * We don't generate composite chunk for it.
   * Instead, we return PutBlobMetaInfo which includes the data chunk list.
   * No matter if router enables "reserved metadata chunk" feature, S3 part upload doesn't reserve metadata chunk.
   * @throws Exception
   */
  @Test
  public void testS3PartUpload() throws Exception {
    int chunkNumber = 9;
    // composite blob with "chunkNumber" of data chunks
    maxPutChunkSize = PUT_CONTENT_SIZE / chunkNumber;
    if (PUT_CONTENT_SIZE % maxPutChunkSize != 0) {
      maxPutChunkSize++;
    }
    setRouter();
    setOperationParams();

    // set skipCompositeChunk to true
    RestRequest request = createRestRequestForPutOperation();
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(request, null, CLUSTER_NAME));
    PutBlobOptions putBlobOptions = new PutBlobOptionsBuilder().restRequest(request).skipCompositeChunk(true).build();
    String compositeBlobInfo = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putBlobOptions)
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    PutBlobMetaInfo metaInfo = PutBlobMetaInfo.deserialize(compositeBlobInfo);

    // Serialize and Deserialize, the PutBlobMetaInfo should be the same
    String metaInfoStr = metaInfo.toString();
    PutBlobMetaInfo deserializedMetaInfo = PutBlobMetaInfo.deserialize(metaInfoStr);

    // verify the deserialized PutBlobMetaInfo
    Assert.assertEquals(chunkNumber, deserializedMetaInfo.getNumChunks());
    // verify the content, one data chunk after another
    List<Pair<String, Long>> chunks = deserializedMetaInfo.getOrderedChunkIdSizeList();
    // reserved metadata chunk id should be null
    Assert.assertNull(deserializedMetaInfo.getReservedMetadataChunkId());
    int chunkSize = maxPutChunkSize;
    for (int i = 0; i < chunkNumber; i++) {
      Pair<String, Long> chunk = chunks.get(i);
      GetBlobResult chunkResult =
          router.getBlob(chunk.getFirst(), new GetBlobOptionsBuilder().build(), null, null).get();
      // last chunk
      if (i == chunkNumber - 1) {
        chunkSize = PUT_CONTENT_SIZE - maxPutChunkSize * i;
      }
      Assert.assertEquals(chunkSize, chunk.getSecond().longValue());

      // verify the reserved metadata chunk id of each data chunk is null
      BlobProperties blobProperties = chunkResult.getBlobInfo().getBlobProperties();
      Assert.assertEquals(blobProperties.getReservedMetadataBlobId(), null);

      // verify data content
      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      chunkResult.getBlobDataChannel().readInto(retainingAsyncWritableChannel, null).get();
      InputStream input = retainingAsyncWritableChannel.consumeContentAsInputStream();
      retainingAsyncWritableChannel.close();
      Assert.assertEquals(chunkSize, input.available());
      byte[] readContent = Utils.readBytesFromStream(input, chunkSize);
      input.close();

      byte[] originSlice = Arrays.copyOfRange(putContent, i * maxPutChunkSize, i * maxPutChunkSize + chunkSize);
      Assert.assertArrayEquals(originSlice, readContent);
    }
  }

  private RestRequest createRestRequestForPutOperation()
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String uri = S3_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + blobName;
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, RestMethod.POST.name());
    request.put(MockRestRequest.URI_KEY, uri);
    return new MockRestRequest(request, null);
  }
}
