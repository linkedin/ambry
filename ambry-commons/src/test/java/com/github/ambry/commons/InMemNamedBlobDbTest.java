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
package com.github.ambry.commons;

import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test class for {@link InMemNamedBlobDb}.
 */
public class InMemNamedBlobDbTest {
  private static final String ACCOUNT_NAME = "testAccount";
  private static final String CONTAINER_NAME = "testContainer";
  private static final String BLOB_NAME = "testBlob";
  private static final String BLOB_ID = "testBlobId";
  private static final long BLOB_SIZE = 1024L;

  /**
   * Test that getting an expired blob without Include_Expired_Blobs option returns Deleted error
   * and does not complete the future successfully (tests the bug fix for missing return statement).
   */
  @Test
  public void testGetExpiredBlobReturnsDeletedError() throws Exception {
    MockTime mockTime = new MockTime(1000L);
    InMemNamedBlobDb db = new InMemNamedBlobDb(mockTime, 100, false);

    // Create a blob with expiration time in the past
    long expirationTime = 500L; // Expired (current time is 1000L)
    NamedBlobRecord record = new NamedBlobRecord(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, BLOB_ID, expirationTime,
        NamedBlobRecord.UNINITIALIZED_VERSION, BLOB_SIZE, 0, false);

    // Put the blob
    CompletableFuture<PutResult> putFuture = db.put(record, NamedBlobState.READY, false);
    putFuture.get(); // Wait for put to complete

    // Try to get the expired blob without Include_Expired_Blobs option
    CompletableFuture<NamedBlobRecord> getFuture = db.get(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, GetOption.None,
        false);

    // Verify that the future completes exceptionally with Deleted error
    try {
      getFuture.get();
      fail("Expected RestServiceException with Deleted error code");
    } catch (ExecutionException e) {
      assertTrue("Exception should be RestServiceException", e.getCause() instanceof RestServiceException);
      RestServiceException restException = (RestServiceException) e.getCause();
      assertEquals("Error code should be Deleted", RestServiceErrorCode.Deleted, restException.getErrorCode());
    }

    // Verify that the future is not completed successfully (this would happen if the return was missing)
    assertTrue("Future should be completed exceptionally", getFuture.isCompletedExceptionally());
    assertFalse("Future should not be completed normally", getFuture.isDone() && !getFuture.isCompletedExceptionally());
  }

  /**
   * Test that getting an expired blob with Include_Expired_Blobs option returns the blob successfully.
   */
  @Test
  public void testGetExpiredBlobWithIncludeExpiredOption() throws Exception {
    MockTime mockTime = new MockTime(1000L);
    InMemNamedBlobDb db = new InMemNamedBlobDb(mockTime, 100, false);

    // Create a blob with expiration time in the past
    long expirationTime = 500L; // Expired (current time is 1000L)
    NamedBlobRecord record = new NamedBlobRecord(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, BLOB_ID, expirationTime,
        NamedBlobRecord.UNINITIALIZED_VERSION, BLOB_SIZE, 0, false);

    // Put the blob
    CompletableFuture<PutResult> putFuture = db.put(record, NamedBlobState.READY, false);
    putFuture.get(); // Wait for put to complete

    // Get the expired blob with Include_Expired_Blobs option
    CompletableFuture<NamedBlobRecord> getFuture = db.get(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME,
        GetOption.Include_Expired_Blobs, false);

    // Verify that the future completes successfully
    NamedBlobRecord result = getFuture.get();
    assertNotNull("Result should not be null", result);
    assertEquals("Blob name should match", BLOB_NAME, result.getBlobName());
    assertEquals("Expiration time should match", expirationTime, result.getExpirationTimeMs());
  }

  /**
   * Test that deleting a non-existent blob returns NotFound error and does not complete successfully
   * (tests the bug fix for missing return statement in delete method).
   */
  @Test
  public void testDeleteNonExistentBlobReturnsNotFoundError() throws Exception {
    MockTime mockTime = new MockTime(1000L);
    InMemNamedBlobDb db = new InMemNamedBlobDb(mockTime, 100, false);

    // Try to delete a blob that doesn't exist
    CompletableFuture<DeleteResult> deleteFuture = db.delete(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME);

    // Verify that the future completes exceptionally with NotFound error
    try {
      deleteFuture.get();
      fail("Expected RestServiceException with NotFound error code");
    } catch (ExecutionException e) {
      assertTrue("Exception should be RestServiceException", e.getCause() instanceof RestServiceException);
      RestServiceException restException = (RestServiceException) e.getCause();
      assertEquals("Error code should be NotFound", RestServiceErrorCode.NotFound, restException.getErrorCode());
    }

    // Verify that the future is not completed successfully (this would happen if the return was missing)
    assertTrue("Future should be completed exceptionally", deleteFuture.isCompletedExceptionally());
    assertFalse("Future should not be completed normally",
        deleteFuture.isDone() && !deleteFuture.isCompletedExceptionally());
  }

  /**
   * Test that deleting an existing blob completes successfully.
   */
  @Test
  public void testDeleteExistingBlob() throws Exception {
    MockTime mockTime = new MockTime(1000L);
    InMemNamedBlobDb db = new InMemNamedBlobDb(mockTime, 100, false);

    // Create and put a blob
    NamedBlobRecord record = new NamedBlobRecord(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, BLOB_ID,
        Utils.Infinite_Time, NamedBlobRecord.UNINITIALIZED_VERSION, BLOB_SIZE, 0, false);
    CompletableFuture<PutResult> putFuture = db.put(record, NamedBlobState.READY, false);
    putFuture.get(); // Wait for put to complete

    // Delete the blob
    CompletableFuture<DeleteResult> deleteFuture = db.delete(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME);

    // Verify that the future completes successfully
    DeleteResult result = deleteFuture.get();
    assertNotNull("Result should not be null", result);
    assertFalse("Blob versions should not be empty", result.getBlobVersions().isEmpty());
  }
}

