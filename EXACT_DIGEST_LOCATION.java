/**
 * EXACT LOCATION FOR DIGEST RETRIEVAL IN routerPutBlobCallback
 * 
 * File: NamedBlobPutHandler.java
 * Method: routerPutBlobCallback (around line 241)
 */

private Callback<String> routerPutBlobCallback(BlobInfo blobInfo) {
  return buildCallback(frontendMetrics.putRouterPutBlobMetrics, blobId -> {
    restRequest.getMetricsTracker().setBytesTransferred(restRequest.getBytesReceived());
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, restRequest.getBlobBytesReceived());
    restResponseChannel.setHeader(RestUtils.Headers.LOCATION, blobId);
    
    // ==================== ADD THESE LINES HERE ====================
    // Get digest AFTER router has finished reading content
    // The digest is now available because router.putBlob() has read the content via readInto()
    byte[] digestBytes = restRequest.getDigest();
    String digest = digestBytes != null ? Base64.encodeBase64URLSafeString(digestBytes) : null;
    
    // Store digest in request args so IdConverter can retrieve it when creating NamedBlobRecord
    if (digest != null) {
      restRequest.setArg(RestUtils.InternalKeys.DIGEST_FOR_NAMED_BLOB, digest);
    }
    // ==================== END OF NEW CODE ====================
    
    String blobIdClean = stripPrefixAndExtension(blobId);
    if (blobInfo.getBlobProperties().getTimeToLiveInSeconds() == Utils.Infinite_Time) {
      // Do ttl update with retryExecutor. Use the blob ID returned from the router instead of the converted ID
      // since the converted ID may be changed by the ID converter.
      String serviceId = blobInfo.getBlobProperties().getServiceId();
      retryExecutor.runWithRetries(retryPolicy,
          callback -> router.updateBlobTtl(restRequest, blobIdClean, serviceId, Utils.Infinite_Time, callback,
              QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false)), this::isRetriable,
          routerTtlUpdateCallbackForPut(blobInfo));
    } else {
      if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
        //Make sure to process response after delete finished
        updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount(
            deleteDatasetVersionOutOfRetentionCallback(blobInfo));
      } else {
        securityService.processResponse(restRequest, restResponseChannel, blobInfo,
            securityProcessResponseCallback());
      }
    }
  }, uri, LOGGER, deleteDatasetCallback);
}

/**
 * ALSO UPDATE AmbryIdConverterFactory.java line 218-220 to:
 */

// In AmbryIdConverterFactory.convertId() method, replace lines 218-220:

// OLD CODE (line 218-220):
byte[] digestBytes = restRequest.getDigest();
String digest = digestBytes != null ? Base64.encodeBase64URLSafeString(digestBytes) : null;

// NEW CODE:
// Try to get digest from request args first (set by router callback), otherwise try getDigest()
String digest = (String) restRequest.getArgs().get(RestUtils.InternalKeys.DIGEST_FOR_NAMED_BLOB);
if (digest == null) {
  byte[] digestBytes = restRequest.getDigest();
  digest = digestBytes != null ? Base64.encodeBase64URLSafeString(digestBytes) : null;
}

/**
 * ALSO ADD TO RestUtils.InternalKeys:
 */

// In RestUtils.java, add this constant to the InternalKeys class:
public static final String DIGEST_FOR_NAMED_BLOB = "ambry-internal-digest-for-named-blob";

