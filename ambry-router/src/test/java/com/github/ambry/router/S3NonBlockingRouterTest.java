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
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.frontend.PutBlobMetaInfo;
import com.github.ambry.frontend.s3.S3MultipartETag;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.frontend.s3.S3MessagePayload.*;


/**
 * Class to test the {@link NonBlockingRouter} for S3.
 */
@RunWith(Parameterized.class)
public class S3NonBlockingRouterTest extends NonBlockingRouterTestBase {
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String S3_PREFIX = "/s3";
  private static final String SLASH = "/";
  private static final Random random = new Random();

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
   * Test the S3 Multipart upload
   * 1. Part Upload: Test the PutBlob with skipCompositeChunk option. We don't generate composite chunk for it.
   * Instead, we return PutBlobMetaInfo which includes the data chunk list.
   * No matter if router enables "reserved metadata chunk" feature, S3 part upload doesn't reserve metadata chunk.
   * 2. Stitch Blob: It's called in completeMultipartUpload. It stitches all data chunks.
   * @throws Exception
   */
  @Test
  public void testS3MultiPartUpload() throws Exception {
    int chunkNumber = 9;
    // composite blob with "chunkNumber" of data chunks
    maxPutChunkSize = PUT_CONTENT_SIZE / chunkNumber;
    if (PUT_CONTENT_SIZE % maxPutChunkSize != 0) {
      maxPutChunkSize++;
    }
    setRouter();
    setOperationParams();

    int numParts = 2;
    Part parts[] = new Part[numParts];
    for (int part = 0; part < numParts; part++) {
      // set skipCompositeChunk to true
      RestRequest request = createRestRequestForPutOperation();
      request.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(request, null, CLUSTER_NAME));
      PutBlobOptions putBlobOptions = new PutBlobOptionsBuilder().restRequest(request).skipCompositeChunk(true).build();
      ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
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

      parts[part] = new Part(Integer.toString(part + 1), compositeBlobInfo);
    }

    // Complete Multipart Upload with the stitch command
    CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload(parts);

    RestRequest request = createRestRequestForPutOperation();
    PutBlobOptions options = new PutBlobOptionsBuilder().restRequest(request).build();
    List<ChunkInfo> chunksToStitch = getChunksToStitch(completeMultipartUpload);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(request, null, CLUSTER_NAME));
    String blobId = router.stitchBlob(putBlobProperties, putUserMetadata, chunksToStitch, options).get();

    router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get();
  }

  @Test
  public void testS3MultipartETag() throws Exception {
    List<Pair<String, Long>> orgList = new ArrayList<>();
    int chunkCount = 100;
    for (int i = 0; i < chunkCount; i++) {
      orgList.add(new Pair<>(String.valueOf(random.nextLong()), random.nextLong()));
    }

    S3MultipartETag orgETag = new S3MultipartETag(orgList);

    // Serialize and Deserialize, the S3MultipartETag should be the same
    String eTagstr = S3MultipartETag.serialize(orgETag);
    S3MultipartETag deserializedETag = S3MultipartETag.deserialize(eTagstr);
    Assert.assertEquals(orgETag, deserializedETag);
    List<Pair<String, Long>> deserializedList = deserializedETag.getOrderedChunkIdSizeList();
    for (int i = 0; i < chunkCount; i++) {
      Assert.assertEquals(orgList.get(i).getFirst(), deserializedList.get(i).getFirst());
      Assert.assertEquals(orgList.get(i).getSecond(), deserializedList.get(i).getSecond());
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

  List<ChunkInfo> getChunksToStitch(CompleteMultipartUpload completeMultipartUpload) throws RestServiceException {
    // Get parts in order from CompleteMultipartUpload, deserialize each part id to get data chunk ids.
    List<ChunkInfo> chunkInfos = new ArrayList<>();
    try {
      // sort the list in order
      List<Part> sortedParts = Arrays.asList(completeMultipartUpload.getPart());
      Collections.sort(sortedParts, Comparator.comparingInt(Part::getPartNumber));

      String reservedMetadataId = null;
      for (Part part : sortedParts) {
        PutBlobMetaInfo putBlobMetaInfoObj = PutBlobMetaInfo.deserialize(part.geteTag());
        // reservedMetadataId can be null. but if it's not null, they are supposed to be the same
        String reserved = putBlobMetaInfoObj.getReservedMetadataChunkId();
        if (reservedMetadataId == null) {
          reservedMetadataId = reserved;
        }
        if (reserved != null && !reserved.equals(reservedMetadataId)) {
          String error = "Reserved ID are different " + completeMultipartUpload;
          throw new RestServiceException(error, RestServiceErrorCode.BadRequest);
        }
        long expirationTimeInMs = -1;

        List<Pair<String, Long>> chunks = putBlobMetaInfoObj.getOrderedChunkIdSizeList();
        for (int i = 0; i < putBlobMetaInfoObj.getNumChunks(); i++) {
          String blobId = chunks.get(i).getFirst();
          long chunkSize = chunks.get(i).getSecond();

          ChunkInfo chunk = new ChunkInfo(blobId, chunkSize, expirationTimeInMs, reservedMetadataId);
          chunkInfos.add(chunk);
        }
      }
    } catch (IOException e) {
      String error = "Could not parse xml request body " + completeMultipartUpload;
      throw new RestServiceException(error, e, RestServiceErrorCode.BadRequest);
    }
    return chunkInfos;
  }
}
