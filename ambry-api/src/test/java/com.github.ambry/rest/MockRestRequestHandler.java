package com.github.ambry.rest;

/**
 * Implementation of {@link RestRequestHandler} that can be used in tests.
 * <p/>
 * This implementation simply calls the appropriate method in the underlying {@link BlobStorageService}
 * on {@link MockRestRequestHandler#handleRequest(RestRequestInfo)}. If the implementation of {@link BlobStorageService}
 * is blocking, then this will be blocking too.
 */
public class MockRestRequestHandler implements RestRequestHandler {
  public static String THROW_EXCEPTION_ON_REQUEST_COMPLETE_URI = "requestHandlerThrowExceptionOnRequestComplete";

  private final BlobStorageService blobStorageService;
  private boolean isRunning = false;

  public MockRestRequestHandler(BlobStorageService blobStorageService) {
    this.blobStorageService = blobStorageService;
  }

  @Override
  public void start()
      throws InstantiationException {
    isRunning = true;
  }

  @Override
  public void shutdown() {
    isRunning = false;
  }

  /**
   * Calls the appropriate method in the {@link BlobStorageService}. Non-blocking nature depends on the implementation
   * of the underlying {@link BlobStorageService}.
   * @param restRequestInfo the {@link RestRequestInfo} that needs to be handled.
   * @throws RestServiceException
   */
  @Override
  public void handleRequest(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    RestMethod restMethod = restRequestInfo.getRestRequest().getRestMethod();
    switch (restMethod) {
      case GET:
        //blobStorageService.handleGet(restRequestInfo);
        break;
      case POST:
        //blobStorageService.handlePost(restRequestInfo);
        break;
      case DELETE:
        //blobStorageService.handleDelete(restRequestInfo);
        break;
      case HEAD:
        //blobStorageService.handleHead(restRequestInfo);
        break;
      default:
        throw new RestServiceException("Unknown rest method - " + restMethod,
            RestServiceErrorCode.UnsupportedRestMethod);
    }
  }

  @Override
  public void onRequestComplete(RestRequest restRequest) {
    if (restRequest != null && THROW_EXCEPTION_ON_REQUEST_COMPLETE_URI.equals(restRequest.getUri())) {
      throw new RuntimeException(THROW_EXCEPTION_ON_REQUEST_COMPLETE_URI);
    }
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }
}
