/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer.handler;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.filetransfer.utils.OperationRetryHandler;
import com.github.ambry.filetransfer.workflow.GetMetadataWorkflow;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreException;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.filetransfer.FileCopyInfo;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.protocol.FileCopyGetMetaDataResponse;
import com.github.ambry.server.StoreManager;
import java.io.IOException;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link StoreFileCopyHandler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class StoreFileCopyHandlerTest {
  @Mock
  private ConnectionPool connectionPool;

  @Mock
  private StoreManager storeManager;

  @Mock
  private ClusterMap clusterMap;

  @Mock
  private FileCopyInfo fileCopyInfo;

  @Mock
  private OperationRetryHandler retryHandler;

  @Mock
  private final FileCopyGetMetaDataResponse metadataResponse = new FileCopyGetMetaDataResponse(ServerErrorCode.NoError);

  private StoreFileCopyHandler handler;
  private GetMetadataWorkflow getMetadataWorkflow;

  /**
   * Set up the pre-requisites:
   *  create a {@link StoreFileCopyHandler} instance.
   *  create a {@link GetMetadataWorkflow} instance.
   * @throws StoreException
   */
  @Before
  public void setUp() throws StoreException {
    FileCopyHandlerConfig fileCopyHandlerConfig = new FileCopyHandlerConfig(new VerifiableProperties(new Properties()));

    handler = new StoreFileCopyHandler(connectionPool, storeManager, clusterMap, fileCopyHandlerConfig);
    handler.setOperationRetryHandler(retryHandler);
    handler.start();

    getMetadataWorkflow = new GetMetadataWorkflow(connectionPool, fileCopyInfo, fileCopyHandlerConfig);
  }

  /**
   * Clean up the pre-requisites: stop the {@link StoreFileCopyHandler} instance.
   * @throws Exception
   */
  @After
  public void tearDown() throws Exception {
    if (handler != null) {
      handler.stop();
    }
  }

  /**
   * Test the {@link StoreFileCopyHandler#getFileCopyGetMetaDataResponse(FileCopyInfo)} method.
   * The test verifies that the method returns the expected {@link FileCopyGetMetaDataResponse} object.
   * @throws Exception
   */
  @Test
  public void testGetFileCopyGetMetaDataResponseExpectSuccess() throws Exception {
    // Arrange: Mock successful metadata retrieval
    when(handler.getOperationRetryHandler().executeWithRetry(any(), eq(GetMetadataWorkflow.GET_METADATA_OPERATION_NAME)))
        .thenReturn(metadataResponse);

    // Act: Call getFileCopyGetMetaDataResponse
    FileCopyGetMetaDataResponse response = handler.getFileCopyGetMetaDataResponse(fileCopyInfo);

    // Assert: The response is not null and is the same as the metadataResponse
    assertNotNull(response);
    assertEquals(metadataResponse, response);
    verify(handler.getOperationRetryHandler(), times(1)).executeWithRetry(any(),
        eq(GetMetadataWorkflow.GET_METADATA_OPERATION_NAME));
  }

  /**
   * Test the {@link StoreFileCopyHandler#getFileCopyGetMetaDataResponse(FileCopyInfo)} method.
   * The test verifies that the method throws a {@link FileCopyHandlerException} with the expected error code and message.
   * The test simulates an {@link IOException} during metadata retrieval.
   * @throws Exception
   */
  @Test
  public void testGetFileCopyGetMetaDataResponseExpectIOException() throws Exception {
    // Arrange: Mock IOException during metadata retrieval
    when(handler.getOperationRetryHandler().executeWithRetry(any(), eq(GetMetadataWorkflow.GET_METADATA_OPERATION_NAME)))
        .thenThrow(new IOException("Test IO error"));

    // Act: Call getFileCopyGetMetaDataResponse
    // Assert: A FileCopyHandlerException is thrown with the expected error code and message
    // Expected exception error code is FileCopyHandlerGetMetadataApiError
    try {
      handler.getFileCopyGetMetaDataResponse(fileCopyInfo);
      fail("Expected FileCopyHandlerException");
    } catch (FileCopyHandlerException e) {
      assertEquals(FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError, e.getErrorCode());
      assertTrue(e.getMessage().contains("IO error while fetching metadata for file"));
    }
  }

  /**
   * Test the {@link StoreFileCopyHandler#getFileCopyGetMetaDataResponse(FileCopyInfo)} method.
   * The test verifies that the method throws a {@link FileCopyHandlerException} with the expected error code and message.
   * The test simulates a {@link ConnectionPoolTimeoutException} during metadata retrieval.
   * @throws Exception
   */
  @Test
  public void testGetFileCopyGetMetaDataResponseExpectConnectionTimeout() throws Exception {
    // Arrange: Mock ConnectionPoolTimeoutException
    when(handler.getOperationRetryHandler().executeWithRetry(any(), eq(GetMetadataWorkflow.GET_METADATA_OPERATION_NAME)))
        .thenThrow(new ConnectionPoolTimeoutException("Timeout"));

    // Act: Call getFileCopyGetMetaDataResponse
    // Assert: A FileCopyHandlerException is thrown with the expected error code and message
    // Expected exception error code is FileCopyHandlerGetMetadataApiError
    try {
      handler.getFileCopyGetMetaDataResponse(fileCopyInfo);
      fail("Expected FileCopyHandlerException");
    } catch (FileCopyHandlerException e) {
      assertEquals(FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError, e.getErrorCode());
      assertTrue(e.getMessage().contains("Connection pool timeout while fetching metadata"));
    }
  }

  /**
   * Test the {@link StoreFileCopyHandler#getFileCopyGetMetaDataResponse(FileCopyInfo)} method.
   * The test verifies that the method throws a {@link FileCopyHandlerException} with the expected error code and message.
   * The test simulates an {@link InterruptedException} during metadata retrieval.
   * @throws Exception
   */
  @Test
  public void testGetFileCopyGetMetaDataResponseExpectInterruptedException() throws Exception {
    // Arrange: Mock InterruptedException
    when(handler.getOperationRetryHandler().executeWithRetry(any(), eq(GetMetadataWorkflow.GET_METADATA_OPERATION_NAME)))
        .thenThrow(new InterruptedException("Interrupted"));

    // Act: Call getFileCopyGetMetaDataResponse
    // Assert: A FileCopyHandlerException is thrown with the expected error code and message
    // Expected exception error code is FileCopyHandlerGetMetadataApiError
    try {
      handler.getFileCopyGetMetaDataResponse(fileCopyInfo);
      fail("Expected FileCopyHandlerException");
    } catch (FileCopyHandlerException e) {
      assertEquals(FileCopyHandlerException.FileCopyHandlerErrorCode.FileCopyHandlerGetMetadataApiError, e.getErrorCode());
      assertTrue(e.getMessage().contains("Thread interrupted while fetching metadata"));
      assertTrue(Thread.currentThread().isInterrupted()); // Ensure the interrupt flag is set
    }
  }

  /**
   * Test the {@link StoreFileCopyHandler#getFileCopyGetMetaDataResponse(FileCopyInfo)} method.
   * The test verifies that the method throws a {@link FileCopyHandlerException} with the expected error code and message.
   * The test simulates an unexpected {@link RuntimeException} during metadata retrieval.
   * @throws Exception
   */
  @Test
  public void testGetFileCopyGetMetaDataResponseExpectRuntimeException() throws Exception {
    // Mock unexpected RuntimeException
    when(handler.getOperationRetryHandler().executeWithRetry(any(), eq(GetMetadataWorkflow.GET_METADATA_OPERATION_NAME)))
        .thenThrow(new RuntimeException("Unexpected error"));

    // Act: Call getFileCopyGetMetaDataResponse
    // Assert: A FileCopyHandlerException is thrown with the expected error code and message
    // Expected exception error code is FileCopyHandlerGetMetadataApiError
    try {
      handler.getFileCopyGetMetaDataResponse(fileCopyInfo);
      fail("Expected FileCopyHandlerException");
    } catch (FileCopyHandlerException e) {
      assertEquals(FileCopyHandlerException.FileCopyHandlerErrorCode.UnknownError, e.getErrorCode());
      assertTrue(e.getMessage().contains("Unexpected runtime error while fetching metadata"));
    }
  }
}
