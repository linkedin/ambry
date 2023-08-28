package com.github.ambry.frontend;

import com.azure.core.implementation.logging.DefaultLogger;
import com.github.ambry.account.Container;
import com.github.ambry.account.Dataset;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * {@link ClientMappingHandler} is designed to handle the storage of names of blobs.
 *
 * In the {@link #handle} method, we would replace the named blob id in the {@link RequestPath} with the
 * corresponding regular blob id.
 */
public class ClientMappingHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientMappingHandler.class);
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics frontendMetrics;
  private final NamedBlobDb namedBlobDb;
  private FrontendConfig frontendConfig;

  /**
   * Construct a {@link ClientMappingHandler} for handling requests for getting blobs in named blob accounts.
   * @param accountAndContainerInjector The {@link AccountAndContainerInjector} to use.
   * @param namedBlobDb The {@link NamedBlobDb} to use.
   * @param metrics The {@link FrontendMetrics} instance where metrics are recorded
   */
  ClientMappingHandler(AccountAndContainerInjector accountAndContainerInjector, NamedBlobDb namedBlobDb,
      FrontendMetrics metrics, FrontendConfig frontendConfig) {
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.namedBlobDb = namedBlobDb;
    this.frontendMetrics = metrics;
    this.frontendConfig = frontendConfig;
  }

  /**
   * Asynchronously handle named blob get requests.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    String blobName = restRequest.getArgs().get("x-ambry-blob-name").toString();
    String accountName = restRequest.getArgs().get("x-ambry-target-account-name").toString();
    String containerName = restRequest.getArgs().get("x-ambry-target-container-name").toString();
    LOGGER.info("Blob name is: " + blobName + ", account name is: " + accountName + ", container name is: " + containerName);

    // cases: get/put/delete
    if(restRequest.getRestMethod().equals(RestMethod.GET)){
      LOGGER.info("Get mapping handler is working.");
      GetOption getOption = getGetOption(restRequest, GetOption.None);
      namedBlobDb.get(accountName, containerName, blobName,
          getOption).thenApply(NamedBlobRecord::getBlobId).whenComplete((blobId, exception) -> {
        if (exception != null) {
          callback.onCompletion(null, Utils.extractFutureExceptionCause(exception));
          return;
        }
        LOGGER.info("Get operation is completed. Blob id is: " + blobId);
        restResponseChannel.setHeader("x-ambry-blob-id", blobId);
        callback.onCompletion(null, null);
      });
    }else if(restRequest.getRestMethod().equals(RestMethod.PUT)){
      LOGGER.info("Put mapping handler is working.");
      String blobId = restRequest.getArgs().get("x-ambry-blob-id").toString();
      long expirationTimeMs =
          Utils.addSecondsToEpochTime(SystemTime.getInstance().milliseconds(), getTtlFromRequestHeader(restRequest.getArgs()));
      NamedBlobRecord record = new NamedBlobRecord(accountName, containerName, blobName, blobId, expirationTimeMs);
      NamedBlobState state = NamedBlobState.READY;
      namedBlobDb.put(record, state, RestUtils.isUpsertForNamedBlob(restRequest.getArgs()));
      LOGGER.info("Put operation is completed.");
      callback.onCompletion(null, null);
    }else if(restRequest.getRestMethod().equals(RestMethod.DELETE)){
      LOGGER.info("Delete mapping handler is working.");
      namedBlobDb.delete(accountName, containerName, blobName)
          .thenApply(DeleteResult::getBlobId)
          .whenComplete((blobId, exception) -> {
            if (exception != null) {
              Exception dbException = Utils.extractFutureExceptionCause(exception);
              callback.onCompletion(null, dbException);
              return;
            }
            LOGGER.info("Delete operation is completed. Blob id is: " + blobId);
            restResponseChannel.setHeader("x-ambry-blob-id", blobId);
            callback.onCompletion(null, null);
          });
    }
  }

  /**
   * An util method to get ttl from arguments associated with a request.
   * @param args the arguments associated with the request. Cannot be {@code null}.
   * @return the ttl extracted from the arguments.
   * @throws RestServiceException if required arguments aren't present or if they aren't in the format expected.
   */
  public static long getTtlFromRequestHeader(Map<String, Object> args) throws RestServiceException {
    long ttl = Utils.Infinite_Time;
    Long ttlFromHeader = getLongHeader(args, Headers.TTL, false);
    if (ttlFromHeader != null) {
      if (ttlFromHeader < -1) {
        throw new RestServiceException(Headers.TTL + "[" + ttlFromHeader + "] is not valid (has to be >= -1)",
            RestServiceErrorCode.InvalidArgs);
      }
      ttl = ttlFromHeader;
    }
    return ttl;
  }
}
