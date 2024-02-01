/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.s3.S3BaseHandler;
import com.github.ambry.frontend.s3.S3MultipartUploadHandler;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory that instantiates an {@link IdConverter} implementation for the frontend.
 */
public class AmbryIdConverterFactory implements IdConverterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmbryIdConverterFactory.class);
  private final IdSigningService idSigningService;
  private final NamedBlobDb namedBlobDb;
  private final FrontendMetrics frontendMetrics;

  public AmbryIdConverterFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      IdSigningService idSigningService, NamedBlobDb namedBlobDb) {
    this.idSigningService = idSigningService;
    this.namedBlobDb = namedBlobDb;
    frontendMetrics = new FrontendMetrics(metricRegistry, new FrontendConfig(verifiableProperties));
  }

  @Override
  public IdConverter getIdConverter() {
    return new AmbryIdConverter(idSigningService, namedBlobDb, frontendMetrics);
  }

  static class AmbryIdConverter implements IdConverter {
    private boolean isOpen = true;
    private final IdSigningService idSigningService;
    private final NamedBlobDb namedBlobDb;
    private final FrontendMetrics frontendMetrics;

    AmbryIdConverter(IdSigningService idSigningService, NamedBlobDb namedBlobDb, FrontendMetrics frontendMetrics) {
      this.idSigningService = idSigningService;
      this.namedBlobDb = namedBlobDb;
      this.frontendMetrics = frontendMetrics;
    }

    @Override
    public void close() {
      isOpen = false;
    }

    @Override
    public Future<String> convert(RestRequest restRequest, String input, Callback<String> callback) {
      return convert(restRequest, input, null, callback);
    }

    /**
     * {@inheritDoc}
     * On {@link RestMethod#POST}, adds a leading slash to indicate that the ID represents the path of the resource
     * created.
     * On any other {@link RestMethod}, removes the leading slash in order to convert the path into an ID that the
     * {@link com.github.ambry.router.Router} will understand.
     * @param restRequest {@link RestRequest} representing the request.
     * @param input the ID that needs to be converted.
     * @param blobInfo the {@link BlobInfo} for an uploaded blob. This will be used for named blob PUT requests.
     * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
     * @return a {@link Future} that will eventually contain the converted ID.
     */
    @Override
    public Future<String> convert(RestRequest restRequest, String input, BlobInfo blobInfo, Callback<String> callback) {
      final CompletableFuture<String> future = new CompletableFuture<>();
      String convertedId = null;
      Exception exception = null;
      frontendMetrics.idConverterRequestRate.mark();
      long startTimeInMs = System.currentTimeMillis();
      try {
        if (!isOpen) {
          exception = new RestServiceException("IdConverter is closed", RestServiceErrorCode.ServiceUnavailable);
        } else if (restRequest.getRestMethod().equals(RestMethod.POST) && !RestUtils.isS3Request(restRequest)) {
          // Ambry chunk uploads come as POST requests. Sign id if required.
          // TODO [S3] Add ID conversion for S3 POST requests (coming during multipart uploads) as well
          convertedId = "/" + signIdIfRequired(restRequest, input);
        } else {
          CallbackUtils.callCallbackAfter(convertId(input, restRequest, blobInfo),
              (id, e) -> completeConverterOperation(id, e, future, callback,
                  frontendMetrics.idConversionDownstreamCallbackTimeInMs));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        frontendMetrics.idConverterProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
        if (convertedId != null || exception != null) {
          completeConverterOperation(convertedId, exception, future, callback, frontendMetrics.idConversionDownstreamCallbackTimeInMs);
        }
      }
      return future;
    }

    @Override
    public Future<Boolean> detectConflict(RestRequest restRequest, Callback<Boolean> callback) {
      final CompletableFuture<Boolean> future = new CompletableFuture<>();
      Exception exception = null;
      frontendMetrics.idConverterDetectConflictRate.mark();
      long startTimeInMs = System.currentTimeMillis();
      try {
        if (!isOpen) {
          exception = new RestServiceException("IdConverter is closed", RestServiceErrorCode.ServiceUnavailable);
        } else {
          CallbackUtils.callCallbackAfter(detectConflict(restRequest),
              (conflictStatus, e) -> completeConverterOperation(conflictStatus, e, future, callback, frontendMetrics.idConversionDetectConflictDownstreamCallbackTimeInMs));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        frontendMetrics.deleteCallbackProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
        if (exception != null) {
          completeConverterOperation(false, exception, future, callback, frontendMetrics.idConversionDetectConflictDownstreamCallbackTimeInMs);
        }
      }
      return future;
    }

    /**
     * Detects if the ID in the request can cause a conflict.
     * @param restRequest @link RestRequest} representing the request.
     * @return the {@link CompletionStage} that will be completed with the conflict status.
     * @throws RestServiceException if the operation cannot be performed.
     */
    private CompletionStage<Boolean> detectConflict(RestRequest restRequest) throws RestServiceException {
      final CompletionStage<Boolean> detectionFuture;
      frontendMetrics.idConverterDetectConflictRate.mark();
      if (restRequest.getRestMethod() == RestMethod.PUT && RestUtils.getRequestPath(restRequest)
          .matchesOperation(Operations.NAMED_BLOB) && !RestUtils.isUpsertForNamedBlob(restRequest.getArgs())) {
        // A named blob put without upsert is allowed only if the named blob does not exist.
        // If this is a named blob put request without upsert, then we check if a mapping for the name already exists.
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(RestUtils.getRequestPath(restRequest), restRequest.getArgs());

        detectionFuture = getNamedBlobDb().get(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
            namedBlobPath.getBlobName(), GetOption.Include_All).thenApply(namedBlobRecord -> {
          LOGGER.info("A mapping for the namedBlob {} already exists. Original BlobId: {}",
              namedBlobRecord.getBlobName(), namedBlobRecord.getBlobId());
          return true;
        }).exceptionally(e -> {
          if (e.getCause() instanceof RestServiceException
              && ((RestServiceException) e.getCause()).getErrorCode() == RestServiceErrorCode.NotFound) {
            return false;
          } else {
            throw new RuntimeException(e);
          }
        });
      } else {
        detectionFuture = CompletableFuture.completedFuture(false);
      }
      return detectionFuture;
    }

    /**
     * Completes the operation of this converter by setting the future and invoking the callback.
     * @param result the conversion result.
     * @param exception any exception that occurred as a part of the conversion.
     * @param completableFuture the {@link CompletableFuture} that must be set.
     * @param callback the {@link Callback} that needs to be invoked. Can be null.
     * @param downstreamCallbackTimeInMs {@link Histogram} to update the time taken for downstream callback.
     */
    private <T> void completeConverterOperation(T result, Exception exception, CompletableFuture<T> completableFuture,
        Callback<T> callback, Histogram downstreamCallbackTimeInMs) {
      if (exception == null) {
        completableFuture.complete(result);
      } else {
        completableFuture.completeExceptionally(exception);
      }
      if (callback != null) {
        long startTime = System.currentTimeMillis();
        callback.onCompletion(result, exception);
        downstreamCallbackTimeInMs.update(System.currentTimeMillis() - startTime);
      }
    }

    /**
     * Convert the input ID to the requested output. If it's the named blob request, return the blobId from NameBlobDb,
     * otherwise return the input with leading slash and extension be stripped.
     * @param input the input blob ID.
     * @param restRequest the {@link RestRequest} to set arguments in.
     * @param blobInfo the {@link BlobInfo} for an uploaded blob. This will be used for named blob PUT requests.
     * @return the {@link CompletionStage} that will be completed with the converted ID
     * @throws RestServiceException
     */
    private CompletionStage<String> convertId(String input, RestRequest restRequest, BlobInfo blobInfo)
        throws RestServiceException {
      CompletionStage<String> conversionFuture;
      LOGGER.debug("input for convertId : " + input);
      LOGGER.debug("restRequest for convertId : " + restRequest);
      if (RequestPath.matchesOperation(input, Operations.NAMED_BLOB)) {
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(input, Collections.emptyMap());
        GetOption getOption = RestUtils.getGetOption(restRequest, GetOption.None);
        if (restRequest.getRestMethod() == RestMethod.DELETE) {
          // on delete requests we can soft delete the record from NamedBlobDb and get the blob ID in one step.
          conversionFuture = getNamedBlobDb().delete(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
              namedBlobPath.getBlobName()).thenApply(DeleteResult::getBlobId);
        }  else if (restRequest.getRestMethod() == RestMethod.PUT && RestUtils.getRequestPath(restRequest)
            .matchesOperation(Operations.UPDATE_TTL)) {
          //If operation == UPDATE_TTL, we will get the version and blobId info from named blob first
          //and do update ttl in routerCallBack.
          conversionFuture = getNamedBlobDb().get(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
              namedBlobPath.getBlobName(), getOption).thenApply(result -> {
            restRequest.setArg(RestUtils.InternalKeys.NAMED_BLOB_VERSION, result.getVersion());
            return result.getBlobId();
          });
        } else {
          conversionFuture = getNamedBlobDb().get(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
              namedBlobPath.getBlobName(), getOption).thenApply(NamedBlobRecord::getBlobId);
        }
      } else if (isNamedBlobPutRequest(restRequest) || isS3MultipartUploadCompleteRequest(restRequest)) {
        Objects.requireNonNull(blobInfo, "blobInfo cannot be null.");
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(RestUtils.getRequestPath(restRequest), restRequest.getArgs());
        String blobId = RestUtils.stripSlashAndExtensionFromId(input);
        BlobProperties properties = blobInfo.getBlobProperties();
        long expirationTimeMs =
            Utils.addSecondsToEpochTime(properties.getCreationTimeInMs(), properties.getTimeToLiveInSeconds());
        NamedBlobRecord record = new NamedBlobRecord(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
            namedBlobPath.getBlobName(), blobId, expirationTimeMs);
        NamedBlobState state = NamedBlobState.READY;
        if (properties.getTimeToLiveInSeconds() == Utils.Infinite_Time) {
          // Set named blob state as 'IN_PROGRESS', will set the state to be 'READY' in the ttlUpdate success callback: routerTtlUpdateCallback
          state = NamedBlobState.IN_PROGRESS;
        }
        conversionFuture = getNamedBlobDb().put(record, state, RestUtils.isUpsertForNamedBlob(restRequest.getArgs())).thenApply(
            result -> {
              restRequest.setArg(RestUtils.InternalKeys.NAMED_BLOB_VERSION, result.getInsertedRecord().getVersion());
              return result.getInsertedRecord().getBlobId();
            });
      } else {
        String decryptedInput =
            parseSignedIdIfRequired(restRequest, input.startsWith("/") ? input.substring(1) : input);
        conversionFuture = CompletableFuture.completedFuture(RestUtils.stripSlashAndExtensionFromId(decryptedInput));
      }
      return conversionFuture;
    }

    /**
     * @param restRequest incoming {@link RestRequest}
     * @return {@code true} if this named blob PUT request
     */
    private boolean isNamedBlobPutRequest(RestRequest restRequest) {
      return restRequest.getRestMethod() == RestMethod.PUT && RestUtils.getRequestPath(restRequest)
          .matchesOperation(Operations.NAMED_BLOB);
    }

    /**
     * @param restRequest incoming {@link RestRequest}
     * @return {@code true} if this is a S3 multipart completee request
     */
    private boolean isS3MultipartUploadCompleteRequest(RestRequest restRequest) {
      return S3BaseHandler.isMultipartCompleteUploadRequest(restRequest);
    }

    /**
     * If the incoming ID is signed, parse the signed ID and set the
     * {@link RestUtils.InternalKeys#SIGNED_ID_METADATA_KEY} in the {@link RestRequest}. Otherwise, just return the
     * provided ID.
     * @param restRequest the {@link RestRequest} to potentially set the signed ID metadata in.
     * @param incomingId the incoming ID, with the leading slash removed.
     * @return a blob ID that can be passed to the router.
     * @throws RestServiceException if parsing the signed ID fails.
     */
    private String parseSignedIdIfRequired(RestRequest restRequest, String incomingId) throws RestServiceException {
      String blobId;
      if (idSigningService.isIdSigned(incomingId)) {
        Pair<String, Map<String, String>> idAndMetadata = idSigningService.parseSignedId(incomingId);
        restRequest.setArg(RestUtils.InternalKeys.SIGNED_ID_METADATA_KEY, idAndMetadata.getSecond());
        blobId = idAndMetadata.getFirst();
      } else {
        blobId = incomingId;
      }
      return blobId;
    }

    /**
     * If {@link RestUtils.InternalKeys#SIGNED_ID_METADATA_KEY} is set in the {@link RestRequest}, return a signed id.
     * Otherwise, return the provided blob ID.
     * @param restRequest the {@link RestRequest} that might contain the signed ID metadata.
     * @param blobId the blob ID to potentially sign.
     * @return the signed ID, if required. Otherwise, just the provided blob ID.
     * @throws RestServiceException if building the signed ID fails.
     */
    private String signIdIfRequired(RestRequest restRequest, String blobId) throws RestServiceException {
      @SuppressWarnings("unchecked")
      Map<String, String> metadata =
          (Map<String, String>) restRequest.getArgs().get(RestUtils.InternalKeys.SIGNED_ID_METADATA_KEY);
      return metadata != null ? idSigningService.getSignedId(blobId, metadata) : blobId;
    }

    private NamedBlobDb getNamedBlobDb() throws RestServiceException {
      if (namedBlobDb == null) {
        throw new RestServiceException("Named blob support not enabled", RestServiceErrorCode.BadRequest);
      }
      return namedBlobDb;
    }
  }
}
