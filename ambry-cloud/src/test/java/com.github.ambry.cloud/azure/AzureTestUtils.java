/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import rx.Observable;
import rx.observables.BlockingObservable;

import static org.mockito.Mockito.*;


/**
 * Class to define utilities for azure tests.
 */
class AzureTestUtils {

  /**
   * Create {@link Document} object from {@link CloudBlobMetadata} object.
   * @param cloudBlobMetadata {@link CloudBlobMetadata} object.
   * @return {@link Document} object.
   */
  static Document createDocumentFromCloudBlobMetadata(CloudBlobMetadata cloudBlobMetadata, ObjectMapper objectMapper)
      throws IOException {
    Document document = new Document(objectMapper.writeValueAsString(cloudBlobMetadata));
    document.set(CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN, System.currentTimeMillis());
    return document;
  }

  /**
   * Create {@link Document} object from {@link CloudBlobMetadata} object with specified updateTime.
   * @param cloudBlobMetadata {@link CloudBlobMetadata} object.
   * @param uploadTime specified upload time.
   * @return {@link Document} object.
   */
  static Document createDocumentFromCloudBlobMetadata(CloudBlobMetadata cloudBlobMetadata, long uploadTime,
      ObjectMapper objectMapper) throws JsonProcessingException {
    Document document = new Document(objectMapper.writeValueAsString(cloudBlobMetadata));
    document.set(CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN, uploadTime);
    return document;
  }

  /**
   * Utility to mock the call chain to get mocked {@link Observable} for single resource from {@link AsyncDocumentClient}.
   * @return {@link Observable< ResourceResponse <Document>>} object.
   */
  static Observable<ResourceResponse<Document>> getMockedObservableForSingleResource() {
    Observable<ResourceResponse<Document>> mockResponse = mock(Observable.class);
    BlockingObservable<ResourceResponse<Document>> mockBlockingObservable = mock(BlockingObservable.class);
    when(mockResponse.toBlocking()).thenReturn(mockBlockingObservable);
    ResourceResponse<Document> mockResourceResponse = mock(ResourceResponse.class);
    when(mockBlockingObservable.single()).thenReturn(mockResourceResponse);
    Document metadataDoc = new Document();
    when(mockResourceResponse.getResource()).thenReturn(metadataDoc);
    return mockResponse;
  }

  /**
   * Utility to mock the query call chain of {@link AsyncDocumentClient} such that query returns {@code documentList}.
   * @param documentList {@link List <Document>} of documents to return from mocked call.
   * @param mockResponse {@link Observable} mocked response.
   */
  static void mockObservableForQuery(List<Document> documentList, Observable<FeedResponse<Document>> mockResponse) {
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    BlockingObservable<FeedResponse<Document>> mockBlockingObservable = mock(BlockingObservable.class);
    when(mockResponse.toBlocking()).thenReturn(mockBlockingObservable);
    Iterator<FeedResponse<Document>> iterator = mock(Iterator.class);
    when(mockBlockingObservable.getIterator()).thenReturn(iterator);
    when(iterator.hasNext()).thenReturn(true).thenReturn(false);
    when(iterator.next()).thenReturn(feedResponse);
    when(feedResponse.getResults()).thenReturn(documentList);
  }

  /**
   * Utility to mock the query call chain of {@link AsyncDocumentClient} such that query returns empty list.
   */
  static Observable<FeedResponse<Document>> getMockedObservableForQueryWithNoResults() {
    Observable<FeedResponse<Document>> mockResponse = mock(Observable.class);
    BlockingObservable<FeedResponse<Document>> mockBlockingObservable = mock(BlockingObservable.class);
    when(mockResponse.toBlocking()).thenReturn(mockBlockingObservable);
    when(mockBlockingObservable.getIterator()).thenReturn(Collections.emptyIterator());
    return mockResponse;
  }
}
