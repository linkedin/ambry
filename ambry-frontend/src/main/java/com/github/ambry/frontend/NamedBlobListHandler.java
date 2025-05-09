/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.frontend;

import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.s3.S3ListHandler.*;
import static com.github.ambry.rest.RestUtils.*;


/**
 * Handles requests for listing blobs that exist in a named blob container that start with a provided prefix.
 */
public class NamedBlobListHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NamedBlobListHandler.class);

  private final SecurityService securityService;
  private final NamedBlobDb namedBlobDb;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics frontendMetrics;
  private final FrontendConfig frontendConfig;
  private static final String DELIMITER = "/";

  /**
   * Constructs a handler for handling requests for listing blobs in named blob accounts.
   *
   * @param securityService the {@link SecurityService} to use.
   * @param namedBlobDb     the {@link NamedBlobDb} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param frontendConfig {@link FrontendConfig} instance from which to fetch configs.
   */
  NamedBlobListHandler(SecurityService securityService, NamedBlobDb namedBlobDb,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics frontendMetrics,
      FrontendConfig frontendConfig) {
    this.securityService = securityService;
    this.namedBlobDb = namedBlobDb;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.frontendMetrics = frontendMetrics;
    this.frontendConfig = frontendConfig;
  }

  /**
   * Asynchronously get account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final String uri;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#preProcessRequest}.
     */
    private void start() {
      try {
        RestRequestMetrics requestMetrics =
            frontendMetrics.getAccountsMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
        restRequest.getMetricsTracker().injectMetrics(requestMetrics);
        accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest,
            frontendMetrics.getBlobMetricsGroup);
        if (namedBlobDb == null) {
          throw new RestServiceException("Named blob support not enabled", RestServiceErrorCode.BadRequest);
        }
        // Start the callback chain by performing request security processing.
        securityService.processRequest(restRequest, securityProcessRequestCallback());
      } catch (Exception e) {
        finalCallback.onCompletion(null, e);
      }
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * any remaining security checks.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.listSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, make a call to {@link NamedBlobDb#list}.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.listSecurityPostProcessRequestMetrics, securityCheckResult -> {
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(RestUtils.getRequestPath(restRequest), restRequest.getArgs());
        String maxKeys = getHeader(restRequest.getArgs(), MAXKEYS_PARAM_NAME, false);
        // If DELIMITER is not equal to "/" or if it is not enabled in configs, then the directory grouping  is not
        // supported.
        String delimiter = getHeader(restRequest.getArgs(), DELIMITER_PARAM_NAME, false);
        boolean enableDelimiter = delimiter != null && delimiter.equals(DELIMITER) && frontendConfig.enableDelimiter;
        CallbackUtils.callCallbackAfter(
            listRecursively(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
                namedBlobPath.getBlobNamePrefix(), namedBlobPath.getPageToken(),
                maxKeys == null ? frontendConfig.listMaxResults : Integer.parseInt(maxKeys), enableDelimiter),
            listBlobsCallback());
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link NamedBlobDb#list} finishes, serialize the result to JSON and send the response.
     * @return a {@link Callback} to be used with {@link NamedBlobDb#list}.
     */
    private Callback<Page<NamedBlobRecord>> listBlobsCallback() {
      return buildCallback(frontendMetrics.listDbLookupMetrics, page -> {
        ReadableStreamChannel channel =
            serializeJsonToChannel(page.toJson(record -> new NamedBlobListEntry(record).toJson()));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
        finalCallback.onCompletion(channel, null);
      }, uri, LOGGER, finalCallback);
    }

    /**
     *  Top-level recursive method to aggregate results from the underlying NamedBlobDb.list() API
     *  using S3 list objects semantics.
     * <p>
     * This method repeatedly invokes the underlying NamedBlobDb.list() API to retrieve pages of NamedBlobRecord
     * objects and recursively merges them into an aggregated Page until the aggregated Page contains at
     * most {@code maxKey} entries or no further pages exist. When grouping is enabled, entries are examined
     * via {@link #extractDirectory(String, String)} to form aggregated directory entries.
     * @param accountName      the account name.
     * @param containerName    the container name.
     * @param blobNamePrefix   the blob name prefix.
     * @param pageToken        the token for the first page (null for initial call).
     * @param maxKey           the maximum number of entries to accumulate.
     * @param groupDirectories flag indicating whether to group directories.
     * @return a CompletableFuture that, when complete, contains an immutable Page with at most maxKey entries
     *         and an appropriate nextPageToken.
     */
    public CompletableFuture<Page<NamedBlobRecord>> listRecursively(String accountName, String containerName,
        String blobNamePrefix, String pageToken, Integer maxKey, boolean groupDirectories) {

      // Start with an empty aggregated Page.
      Page<NamedBlobRecord> initialAggregatedPage = new Page<>(new ArrayList<>(), null);
      return listRecursivelyInternal(accountName, containerName, blobNamePrefix, pageToken, maxKey, groupDirectories,
          initialAggregatedPage).thenApply(
          finalPage -> new Page<>(finalPage.getEntries(), finalPage.getNextPageToken()));
    }

    /**
     * Internal recursive helper that aggregates pages returned by the underlying list() API into a single Page.
     * <p>
     * This method calls the underlying NamedBlobDb.list() API with the given pageToken and merges the returned Page
     * with the previously aggregated results (contained in {@code aggregatedPage}) using {@link this#mergePageResults}.
     * If the aggregated Page is not yet full (i.e. contains fewer than {@code maxKey} entries) or the last entry is
     * a directory whose blobName prefixes the next page token, the method recurses to merge further entries from that
     * directory; otherwise, it returns the aggregated Page.
     * </p>
     * @param accountName      the account name.
     * @param containerName    the container name.
     * @param blobNamePrefix   the blob name prefix.
     * @param pageToken        the token for the current page.
     * @param maxKey           the maximum number of entries to accumulate.
     * @param groupDirectories flag indicating whether to group directories.
     * @param aggregatedPage   the aggregated Page so far (immutable).
     * @return a CompletableFuture containing a new aggregated Page (immutable) with updated entries and nextPageToken.
     */
    private CompletableFuture<Page<NamedBlobRecord>> listRecursivelyInternal(String accountName, String containerName,
        String blobNamePrefix, String pageToken, int maxKey, boolean groupDirectories,
        Page<NamedBlobRecord> aggregatedPage) {

      return namedBlobDb.list(accountName, containerName, blobNamePrefix, pageToken, maxKey)
          .thenCompose(currentPage -> {
            // Merge the current page into the aggregated page.
            Page<NamedBlobRecord> updatedAggregatedPage =
                mergePageResults(aggregatedPage, currentPage, accountName, containerName, blobNamePrefix, maxKey,
                    groupDirectories);
            String tokenToUse = updatedAggregatedPage.getNextPageToken();

            // If no token is available, no further pages exist. Return the updated aggregated page.
            // This is the final result
            if (tokenToUse == null) {
              return CompletableFuture.completedFuture(updatedAggregatedPage);
            }
            // If we haven't reached maxKey yet, continue fetching.
            if (updatedAggregatedPage.getEntries().size() < maxKey) {
              return listRecursivelyInternal(accountName, containerName, blobNamePrefix, tokenToUse, maxKey,
                  groupDirectories, updatedAggregatedPage);
            } else {
              // Aggregated page is full.
              NamedBlobRecord lastRecord =
                  updatedAggregatedPage.getEntries().get(updatedAggregatedPage.getEntries().size() - 1);
              // If the last record is a directory and tokenToUse starts with that directory's blobName,
              // then continue fetching so that we merge further entries from that directory.
              if (lastRecord.isDirectory() && tokenToUse.startsWith(lastRecord.getBlobName())) {
                return listRecursivelyInternal(accountName, containerName, blobNamePrefix, tokenToUse, maxKey,
                    groupDirectories, updatedAggregatedPage);
              } else {
                // Otherwise, return the updated aggregated page. This is the final result.
                return CompletableFuture.completedFuture(updatedAggregatedPage);
              }
            }
          });
    }

    /**
     * Merges the current page of results into the aggregated Page.
     * <p>
     * This method iterates over the entries of the provided {@code currentPage}. If {@code groupDirectories} is true,
     * each entry is examined via {@link #extractDirectory(String, String)}. If a directory is detected, a new directory
     * record is constructed and added to the aggregated results only if not already present. If adding an entry would
     * cause the aggregated results to reach {@code maxKey} entries, the blobName of that entry is captured as the new
     * page token, and processing of the current page stops.
     * </p>
     *
     * @param aggregatedPage   the aggregated Page so far.
     * @param currentPage      the Page returned from the underlying list call.
     * @param accountName      the account name.
     * @param containerName    the container name.
     * @param blobNamePrefix   the blob name prefix.
     * @param maxKey           the maximum number of entries to accumulate.
     * @param groupDirectories flag indicating whether to group directories.
     * @return a new immutable Page whose entries are the merged entries (accumulator) and whose nextPageToken is
     *         either the blobName of the first unprocessed record or the underlying currentPage's token.
     */
    private Page<NamedBlobRecord> mergePageResults(Page<NamedBlobRecord> aggregatedPage,
        Page<NamedBlobRecord> currentPage, String accountName, String containerName, String blobNamePrefix, int maxKey,
        boolean groupDirectories) {

      // Start with a copy of the current aggregated entries.
      List<NamedBlobRecord> accumulator = new ArrayList<>(aggregatedPage.getEntries());
      String newToken = null;
      List<NamedBlobRecord> entries = currentPage.getEntries();

      for (NamedBlobRecord record : entries) {
        NamedBlobRecord recordToAdd = record;
        if (groupDirectories) {
          String directory = extractDirectory(record.getBlobName(), blobNamePrefix);
          if (directory != null) {
            // Skip duplicate directory records.
            if (accumulator.stream()
                .filter(NamedBlobRecord::isDirectory)
                .anyMatch(existing -> existing.getBlobName().equals(directory))) {
              continue;
            }
            recordToAdd =
                new NamedBlobRecord(accountName, containerName, directory, null, Utils.Infinite_Time, 0, 0, 0, true);
          }
        }
        // If adding this record would reach maxKey, capture its blobName as newToken and stop processing.
        if (accumulator.size() == maxKey) {
          newToken = record.getBlobName();
          break;
        }
        accumulator.add(recordToAdd);
      }
      // Determine the next page token: use newToken if set, else use the token from the underlying currentPage.
      String tokenToUse = (newToken != null) ? newToken : currentPage.getNextPageToken();
      return new Page<>(accumulator, tokenToUse);
    }
  }

  /**
   * Extracts the directory from the given blob name if possible. This method implements logic similar to S3
   * ListObjectsV2 when both a "Prefix" and a "Delimiter" ("/") are provided. It groups keys into common prefixes
   * ("directories"). Below are some examples:
   * 1. If blobName = "abc/def/ghi" and blobNamePrefix = "", then the directory is "abc/".
   * 2. If blobName = "abc/def/ghi" and blobNamePrefix = "abc", then the directory is "abc/".
   * 3. If blobName = "abc/def/ghi" and blobNamePrefix = "abc/", then the directory is "abc/def/".
   * 4. If blobName = "abc//def/ghi" and blobNamePrefix = "abc/", then the directory is "abc//".
   * @param blobName       the blob name from the ResultSet.
   * @param blobNamePrefix the prefix to remove (can be null).
   * @return the directory (with a trailing '/') if found; otherwise, return null.
   */
  private String extractDirectory(String blobName, String blobNamePrefix) {

    // Treat null blobNamePrefix as an empty string.
    blobNamePrefix = (blobNamePrefix == null) ? "" : blobNamePrefix;

    // Since we assume blobNamePrefix is either empty or is contained in blobName, we directly compute the remainder.
    String remainder = blobName.substring(blobNamePrefix.length());
    if (remainder.isEmpty()) {
      return null;
    }

    // If the remainder starts with the delimiter, then per S3 behavior the common prefix is the blobNamePrefix
    // plus an extra delimiter.
    if (remainder.startsWith(DELIMITER)) {
      return blobNamePrefix + DELIMITER;
    }

    // Otherwise, look for the delimiter in the remainder.
    int index = remainder.indexOf(DELIMITER);
    if (index == -1) {
      return null;
    } else {
      // Return the blobNamePrefix plus the substring of the remainder up to and including the delimiter.
      return blobNamePrefix + remainder.substring(0, index + DELIMITER.length());
    }
  }
}
