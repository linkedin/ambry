package com.github.ambry.restservice;

/**
 * Implementation of {@link RestRequestHandler} that can be used in tests.
 * <p/>
 * This implementation simply calls the appropriate method in the underlying {@link BlobStorageService}
 * on {@link MockRestRequestHandler#handleRequest(RestRequestInfo)}. If the implementation of {@link BlobStorageService}
 * is blocking, then this will be blocking too.
 */
public class MockRestRequestHandler implements RestRequestHandler {
  private final BlobStorageService blobStorageService;

  public MockRestRequestHandler(BlobStorageService blobStorageService) {
    this.blobStorageService = blobStorageService;
  }

  @Override
  public void start()
      throws InstantiationException {
    // nothing to do.
  }

  @Override
  public void shutdown() {
    // nothing to do.
  }

  /**
   * Calls the appropriate method in the {@link BlobStorageService}. Non-blocking nature depends on the implementation
   * of the underlying {@link BlobStorageService}.
   * @param restRequestInfo - the {@link RestRequestInfo} that needs to be handled.
   * @throws RestServiceException
   */
  @Override
  public void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    RestMethod restMethod = restRequestInfo.getRestRequestMetadata().getRestMethod();
    switch (restMethod) {
      case GET:
        blobStorageService.handleGet(restRequestInfo);
        break;
      case POST:
        blobStorageService.handlePost(restRequestInfo);
        break;
      case DELETE:
        blobStorageService.handleDelete(restRequestInfo);
        break;
      case HEAD:
        blobStorageService.handleHead(restRequestInfo);
        break;
      default:
        throw new RestServiceException("Unknown rest method - " + restMethod,
            RestServiceErrorCode.UnsupportedRestMethod);
    }
  }

  @Override
  public void onRequestComplete(RestRequestMetadata restRequestMetadata) {
    // nothing to do.
  }
}
