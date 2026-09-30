/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.IdConverterFactory;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class UploadIdConversionTest extends NonBlockingRouterTestBase {
  public UploadIdConversionTest(boolean encrypted) throws Exception {
    super(encrypted, MessageFormatRecord.Metadata_Content_Version_V3, false);
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  @Test
  public void testGeneratedBlobIdConversionErrors() throws Exception {
    IdConverterFactory factory = mock(IdConverterFactory.class);
    IdConverter converter = mock(IdConverter.class);
    when(factory.getIdConverter()).thenReturn(converter);
    setRouterWithIdConverterFactory(getNonBlockingRouterProperties(localDcName),
        new MockServerLayout(mockClusterMap), new LoggingNotificationSystem(), factory);
    setOperationParams();
    String chunkId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, putOptionsForChunkedUpload)
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    for (boolean stitch : new boolean[]{false, true}) {
      for (Exception failure : new Exception[]{
          null,
          new RouterException("invalid ID", RouterErrorCode.InvalidBlobId),
          new RestServiceException("invalid input", RestServiceErrorCode.BadRequest)}) {
        CompletableFuture<Callback<String>> pendingConversion = new CompletableFuture<>();
        FutureResult<String> conversion = new FutureResult<>();
        when(converter.convert(any(), anyString(), any(), any())).thenAnswer(invocation -> {
          pendingConversion.complete(invocation.getArgument(3));
          return conversion;
        });
        setOperationParams();
        try (RestRequest request = createUploadRequest(stitch)) {
          FutureResult<String> result = new FutureResult<>();
          AtomicInteger callbackCount = new AtomicInteger();
          Callback<String> callback = (id, error) -> {
            callbackCount.incrementAndGet();
            result.done(id, error);
          };
          Future<String> stored = stitch ? router.stitchBlob(request, putBlobProperties, putUserMetadata,
              Collections.singletonList(new ChunkInfo(chunkId, PUT_CONTENT_SIZE, Utils.Infinite_Time, null)),
              null, callback, null)
              : router.putBlob(request, putBlobProperties, putUserMetadata, putChannel,
                  new PutBlobOptionsBuilder().build(), callback, null);
          String blobId = stored.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          assertFalse("Upload callback must wait for ID conversion", result.isDone());
          conversion.done(failure == null ? blobId : null, failure);
          Callback<String> conversionCallback = pendingConversion.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          assertEquals(Boolean.TRUE, request.getArgs().get(RestUtils.InternalKeys.BLOB_ID_IS_SERVER_GENERATED));
          conversionCallback.onCompletion(failure == null ? blobId : null, failure);
          if (failure == null) {
            assertEquals(blobId, result.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
          } else {
            assertException(ExecutionException.class, () -> result.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                e -> assertSame(failure, e.getCause()));
          }
          assertEquals(1, callbackCount.get());
          GetBlobResult blob = router.getBlob(blobId, new GetBlobOptionsBuilder().build())
              .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          blob.getBlobDataChannel().close();
        }
      }
    }
    clearInvocations(converter);
    FutureResult<String> result = new FutureResult<>();
    try (RestRequest request = createUploadRequest(true)) {
      router.stitchBlob(request, putBlobProperties, putUserMetadata,
          Collections.singletonList(new ChunkInfo("invalid-client-id", 1, Utils.Infinite_Time, null)),
          null, result::done, null);
      assertException(ExecutionException.class, () -> result.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
          e -> assertEquals(RouterErrorCode.InvalidBlobId, ((RouterException) e.getCause()).getErrorCode()));
      verifyNoInteractions(converter);
      assertNull(request.getArgs().get(RestUtils.InternalKeys.BLOB_ID_IS_SERVER_GENERATED));
    }
  }

  private RestRequest createUploadRequest(boolean stitch) throws Exception {
    JSONObject headers = new JSONObject().put(RestUtils.Headers.UPLOAD_NAMED_BLOB_MODE, stitch ? RestUtils.STITCH : "");
    RestRequest request = new MockRestRequest(new JSONObject().put(MockRestRequest.REST_METHOD_KEY, RestMethod.PUT.name())
        .put(MockRestRequest.URI_KEY, "/named/account/container/blob").put(MockRestRequest.HEADERS_KEY, headers), null);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(request, Collections.emptyList(), null));
    return request;
  }
}
