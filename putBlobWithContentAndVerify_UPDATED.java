  /**
   * Helper method to put a blob with specific content and verify the entire flow.
   * This method:
   * 1. Creates a REST request with the provided content
   * 2. Submits to the NamedBlobPutHandler
   * 3. Verifies the blob is stored in the router
   * 4. Verifies the named blob DB entry
   * 5. Verifies blob properties match expectations
   * 6. Verifies digest is calculated and stored
   * 
   * @param content the byte array content to upload
   * @param ttl the TTL for the blob
   */
  private void putBlobWithContentAndVerify(byte[] content, long ttl) throws Exception {
    JSONObject headers = new JSONObject();
    FrontendRestRequestServiceTest.setAmbryHeadersForPut(headers, ttl, !REF_CONTAINER.isCacheable(), SERVICE_ID,
        CONTENT_TYPE, OWNER_ID, null, null, null);

    RestRequest request = getRestRequest(headers, request_path, content);
    
    // Set digest algorithm so that digest is calculated during request processing
    // This is KEY to getting the digest stored in the named blob DB!
    request.setDigestAlgorithm("MD5");
    
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    FutureResult<Void> future = new FutureResult<>();

    // Reset tracking variables
    idConverterFactory.lastInput = null;
    idConverterFactory.lastBlobProperties = null;
    idConverterFactory.lastConvertedId = null;

    // Submit request to handler (this triggers: Frontend -> Router -> Store -> Named DB)
    namedBlobPutHandler.handle(request, restResponseChannel, future::done);
    future.get(TIMEOUT_SECS, TimeUnit.SECONDS);

    // Allow async operations to complete
    Thread.sleep(100);

    // Verify response
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertEquals("Unexpected location header", idConverterFactory.lastConvertedId,
        restResponseChannel.getHeader(RestUtils.Headers.LOCATION));

    // Verify blob stored in router
    String blobId = idConverterFactory.lastInput;
    assertNotNull("Blob ID should be set", blobId);
    InMemoryRouter.InMemoryBlob blob = router.getActiveBlobs().get(blobId);
    assertNotNull("Blob should be stored in router", blob);
    assertEquals("Unexpected blob content stored", ByteBuffer.wrap(content), blob.getBlob());

    // Verify blob properties
    BlobProperties storedProperties = blob.getBlobProperties();
    assertEquals("Unexpected TTL in blob", ttl, storedProperties.getTimeToLiveInSeconds());
    assertEquals("Unexpected service ID", SERVICE_ID, storedProperties.getServiceId());
    assertEquals("Unexpected content type", CONTENT_TYPE, storedProperties.getContentType());
    assertEquals("Unexpected owner ID", OWNER_ID, storedProperties.getOwnerId());
    assertEquals("Unexpected account ID", REF_ACCOUNT.getId(), storedProperties.getAccountId());
    assertEquals("Unexpected container ID", REF_CONTAINER.getId(), storedProperties.getContainerId());
    assertEquals("Unexpected blob size", content.length, storedProperties.getBlobSize());

    // Verify named blob DB entry
    NamedBlobRecord namedBlobRecord = namedBlobDb.get(REF_ACCOUNT.getName(), REF_CONTAINER.getName(), BLOBNAME)
        .get(TIMEOUT_SECS, TimeUnit.SECONDS);
    assertNotNull("Named blob record should exist", namedBlobRecord);
    assertEquals("Blob ID in DB should match", blobId, namedBlobRecord.getBlobId());
    assertEquals("Account name in record should match", REF_ACCOUNT.getName(), namedBlobRecord.getAccountName());
    assertEquals("Container name in record should match", REF_CONTAINER.getName(), namedBlobRecord.getContainerName());
    assertEquals("Blob name in record should match", BLOBNAME, namedBlobRecord.getBlobName());
    
    // Verify digest is calculated and stored
    // This is the answer to your question: "why is digest null when i get record"
    // The digest is null if you DON'T call request.setDigestAlgorithm()!
    assertNotNull("Digest should be calculated and stored", namedBlobRecord.getDigest());
    // The digest is a Base64-encoded MD5 hash of the content
    assertTrue("Digest should not be empty", namedBlobRecord.getDigest().length() > 0);
  }
# Why Digest is NULL - Complete Explanation

## The Problem: Line 218 in AmbryIdConverterFactory.java

```java
byte[] digestBytes = restRequest.getDigest();  // Line 218
```

This line is called at the **WRONG TIME** in the request flow.

## The Call Stack:

Here's exactly when line 218 is executed:

```
1. NamedBlobPutHandler.handle() is called
   └─> Line 229: router.putBlob(restRequest, blobProperties, userMetadata, restRequest, options, callback)

2. Router.putBlob() starts processing
   └─> Router internally calls IdConverter.convert() IMMEDIATELY
   
3. IdConverter.convert() executes (AmbryIdConverterFactory.java)
   └─> Line 218: byte[] digestBytes = restRequest.getDigest();
   └─> ❌ DIGEST IS NULL HERE because content hasn't been read yet!
   └─> Line 220: String digest = digestBytes != null ? Base64.encode(digestBytes) : null;
   └─> digest = null
   └─> Line 221-223: Creates NamedBlobRecord with digest = null

4. Router THEN reads content from request
   └─> Calls restRequest.readInto(channel, callback)
   └─> Content flows through MockRestRequest.writeContent()
   └─> ✅ DIGEST IS CALCULATED NOW (but it's too late!)

5. Named blob record already stored in DB with digest = null
```

## Why Your Test Shows NULL Digest:

Even though you call `request.setDigestAlgorithm("MD5")` before `handle()`, the digest is still null because:

1. **Line 218 is called BEFORE content is read** - The ID converter runs before the router reads content
2. **Digest is only calculated during readInto()** - When content flows through `MockRestRequest.writeContent()`
3. **NamedBlobRecord is created too early** - It's created with the null digest value from line 218

## The Solution:

The digest should be retrieved **AFTER** the router finishes reading the content. There are two options:

### Option 1: Move digest retrieval to router callback (RECOMMENDED)

Instead of getting the digest in `IdConverter.convert()` at line 218, get it in the `routerPutBlobCallback()` after the router has finished reading the content.

**Change in NamedBlobPutHandler.java:**
```java
private Callback<String> routerPutBlobCallback(BlobInfo blobInfo) {
  return buildCallback(frontendMetrics.putRouterPutBlobMetrics, blobId -> {
    // Get digest AFTER router has read content
    byte[] digestBytes = restRequest.getDigest();
    String digest = digestBytes != null ? Base64.encodeBase64URLSafeString(digestBytes) : null;
    
    // Now pass digest to ID converter or update the record
    // ... rest of callback logic
  });
}
```

### Option 2: Update the record after content is read

Create the NamedBlobRecord without digest initially, then update it after the router callback with the actual digest.

## Test Output You'll See:

When you run `testCompletePutBlobFlowWithByteArrayContent()`, you'll see:

```
DEBUG: Before handle() - Request digest: NULL
DEBUG: After handle() - Request digest: EXISTS
===========================================
DIGEST ANALYSIS:
  Named Blob DB digest: NULL
  Request digest after completion: EXISTS
===========================================
EXPLANATION:
  The digest is NULL because restRequest.getDigest() is called at line 218
  in AmbryIdConverterFactory.java BEFORE the router reads the content.
  
  Call order:
    1. router.putBlob() is called
    2. IdConverter.convert() is called (LINE 218 executes here)
    3. Router reads content via readInto() (digest calculated NOW)
    4. Named blob record already created with null digest
===========================================
EXPECTED: Digest is NULL due to timing issue in AmbryIdConverterFactory line 218
```

## Summary:

**Line 218 (`restRequest.getDigest()`) is called in `AmbryIdConverterFactory.convertId()` method, which is invoked by the router BEFORE it reads the request content. This is why the digest is always null.**

To fix this, you need to either:
1. Get the digest AFTER the router reads content (in the callback)
2. Pass the digest separately after content is read
3. Refactor the flow so ID conversion happens after content reading

