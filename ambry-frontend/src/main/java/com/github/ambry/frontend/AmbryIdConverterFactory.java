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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.commons.Callback;
import com.github.ambry.utils.Pair;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;


/**
 * Factory that instantiates an {@link IdConverter} implementation for the frontend.
 */
public class AmbryIdConverterFactory implements IdConverterFactory {
  private final IdSigningService idSigningService;
  private final FrontendMetrics frontendMetrics;

  public AmbryIdConverterFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      IdSigningService idSigningService, NamedBlobDb namedBlobDb) {
    this.idSigningService = idSigningService;
    frontendMetrics = new FrontendMetrics(metricRegistry);
  }

  @Override
  public IdConverter getIdConverter() {
    return new AmbryIdConverter(idSigningService, frontendMetrics);
  }

  private static class AmbryIdConverter implements IdConverter {
    private boolean isOpen = true;
    private final IdSigningService idSigningService;
    private final FrontendMetrics frontendMetrics;
    private static final String NAMED_BLOB_PREFIX = "/named";

    AmbryIdConverter(IdSigningService idSigningService, FrontendMetrics frontendMetrics) {
      this.idSigningService = idSigningService;
      this.frontendMetrics = frontendMetrics;
    }

    @Override
    public void close() {
      isOpen = false;
    }

    /**
     * {@inheritDoc}
     * On {@link RestMethod#POST}, adds a leading slash to indicate that the ID represents the path of the resource
     * created.
     * On any other {@link RestMethod}, removes the leading slash in order to convert the path into an ID that the
     * {@link com.github.ambry.router.Router} will understand.
     * @param restRequest {@link RestRequest} representing the request.
     * @param input the ID that needs to be converted.
     * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
     * @return a {@link Future} that will eventually contain the converted ID.
     */
    @Override
    public Future<String> convert(RestRequest restRequest, String input, Callback<String> callback) {
      final CompletableFuture<String> future = new CompletableFuture<>();
      String convertedId = null;
      Exception exception = null;
      frontendMetrics.idConverterRequestRate.mark();
      long startTimeInMs = System.currentTimeMillis();
      try {
        if (!isOpen) {
        frontendMetrics.idConverterProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
        exception = new RestServiceException("IdConverter is closed", RestServiceErrorCode.ServiceUnavailable);
        } else if (restRequest.getRestMethod().equals(RestMethod.POST)) {
            convertedId = "/" + signIdIfRequired(restRequest, input);
        } else {
            frontendMetrics.idConverterProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
            convertId(input, restRequest).whenComplete(
                (id, throwable) -> completeConversion(id, extractCompletionExceptionCause(throwable), future, callback));
        }
      } catch (Exception e) {
          exception = e;
      } finally {
        frontendMetrics.idConverterProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
        if (convertedId != null || exception != null) {
          completeConversion(convertedId, exception, future, callback);
        }
      }
      return future;
    }

    /**
     * @param throwable a throwable to possibly wrap in an exception.
     * @return if the {@link Throwable} is an instance of {@link Exception}, return the throwable, otherwise return the
     *         throwable wrapped in an exception.
     */
    private static Exception extractCompletionExceptionCause(Throwable throwable) {
      if (throwable == null) {
        return null;
      }
      if (throwable instanceof CompletionException) {
        throwable = throwable.getCause();
      }
      return throwable instanceof Exception ? (Exception) throwable : new Exception("Encountered throwable", throwable);
    }

    /**
     * Completes the conversion by setting the future and invoking the callback.
     * @param conversionResult the conversion result.
     * @param exception any exception that occurred as a part of the conversion.
     * @param completableFuture the {@link CompletableFuture} that must be set.
     * @param callback the {@link Callback} that needs to be invoked. Can be null.
     */
    private <T> void completeConversion(T conversionResult, Exception exception, CompletableFuture<T> completableFuture,
        Callback<T> callback) {
      if (exception == null) {
        completableFuture.complete(conversionResult);
      } else {
        completableFuture.completeExceptionally(exception);
      }
      if (callback != null) {
        long startTime = System.currentTimeMillis();
        callback.onCompletion(conversionResult, exception);
        frontendMetrics.idConversionDownstreamCallbackTimeInMs.update(System.currentTimeMillis() - startTime);
      }
    }

    /**
     * Convert the input ID to the requested output. If it's the named blob request, return the blobId from NameBlobDb,
     * otherwise return the input with leading slash and extension be stripped.
     * @param input the input blob ID.
     * @param restRequest the {@link RestRequest} to set arguments in.
     * @return the {@link CompletionStage} that will be completed with the converted ID
     * @throws RestServiceException
     */
    private CompletionStage<String> convertId(String input, RestRequest restRequest) throws RestServiceException {
      CompletionStage<String> conversionFuture;
      if (input.startsWith(NAMED_BLOB_PREFIX)) {
        NamedBlobPath namedBlobPath = RestUtils.parseInput(input);
        //will update this hack version once NamedBlobDb is in.
        conversionFuture =
            get(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(), namedBlobPath.getBlobName());
      } else if (restRequest.getRestMethod().equals(RestMethod.PUT) && restRequest.getUri()
          .startsWith(NAMED_BLOB_PREFIX)) {
        NamedBlobPath namedBlobPath =
            RestUtils.parseInput(RestUtils.getRequestPath(restRequest).getOperationOrBlobId(false));
        String blobId = RestUtils.stripSlashAndExtensionFromId(input);
        conversionFuture =
            put(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(), namedBlobPath.getBlobName(), blobId);
      } else {
        String decryptedInput =
            parseSignedIdIfRequired(restRequest, input.startsWith("/") ? input.substring(1) : input);
        conversionFuture = CompletableFuture.completedFuture(RestUtils.stripSlashAndExtensionFromId(decryptedInput));
      }
      return conversionFuture;
    }

    /*
     * will update this hack version once NamedBlobDb is in.
     */
    private CompletableFuture<String> get(String accountName, String containerName, String blobName) {
      CompletableFuture<String> completableFuture = new CompletableFuture<>();
      completableFuture.complete(accountName + containerName + blobName);
      return completableFuture;
    }

    /*
     * will update this hack version once NamedBlobDb is in.
     */
    private CompletableFuture<String> put(String accountName, String containerName, String blobName, String blobId) {
      CompletableFuture<String> completableFuture = new CompletableFuture<>();
      completableFuture.complete(blobId);
      return completableFuture;
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
  }
}
