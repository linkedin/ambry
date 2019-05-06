/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.utils.Pair;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Class to hold different {@link RestRequestMetrics} for plain text, ssl, with and without encryption.
 */
class RestRequestMetricsGroup {
  private static final String SSL_SUFFIX = "Ssl";
  private static final String ENCRYPTED_SUFFIX = "Encrypted";

  private final ConcurrentMap<Pair<String, String>, ContainerMetrics> instantiatedContainerMetrics =
      new ConcurrentHashMap<>();
  private final String requestType;
  private final boolean encryptedMetricsEnabled;
  private final boolean containerMetricsEnabled;
  private final MetricRegistry metricRegistry;
  private final RestRequestMetrics nonSslUnencryptedMetrics;
  private final RestRequestMetrics sslUnencryptedMetrics;
  private final RestRequestMetrics nonSslEncryptedMetrics;
  private final RestRequestMetrics sslEncryptedMetrics;

  /**
   * Instantiates {@link RestRequestMetricsGroup} for the given requestType and resource
   * @param ownerClass the {@link Class} that is supposed to own the metrics created by this group.
   * @param requestType the type of request for which a group is being created.
   * @param encryptedMetricsEnabled {@code true} if separate {@link RestRequestMetrics} should be used for encrypted
   *                                blob operations.
   * @param containerMetricsEnabled {@code true} if {@link ContainerMetrics} should be registered for this operation.
   * @param metricRegistry the {@link MetricRegistry} instance.
   */
  RestRequestMetricsGroup(Class ownerClass, String requestType, boolean encryptedMetricsEnabled,
      boolean containerMetricsEnabled, MetricRegistry metricRegistry) {
    this.requestType = requestType;
    this.encryptedMetricsEnabled = encryptedMetricsEnabled;
    this.containerMetricsEnabled = containerMetricsEnabled;
    this.metricRegistry = metricRegistry;
    nonSslUnencryptedMetrics = new RestRequestMetrics(ownerClass, requestType, metricRegistry);
    sslUnencryptedMetrics = new RestRequestMetrics(ownerClass, requestType + SSL_SUFFIX, metricRegistry);
    nonSslEncryptedMetrics =
        encryptedMetricsEnabled ? new RestRequestMetrics(ownerClass, requestType + ENCRYPTED_SUFFIX, metricRegistry)
            : null;
    sslEncryptedMetrics =
        encryptedMetricsEnabled ? new RestRequestMetrics(ownerClass, requestType + SSL_SUFFIX + ENCRYPTED_SUFFIX,
            metricRegistry) : null;
  }

  /**
   * Fetches the appropriate {@link RestRequestMetrics} based on the params
   * @param sslUsed {@code true} if the request is sent over ssl. {@code false} otherwise
   * @param encrypted {@code true} if the blob is encrypted. {@code false} otherwise
   * @return the appropriate {@link RestRequestMetrics} based on the params
   */
  RestRequestMetrics getRestRequestMetrics(boolean sslUsed, boolean encrypted) {
    if (encrypted && encryptedMetricsEnabled) {
      return sslUsed ? sslEncryptedMetrics : nonSslEncryptedMetrics;
    } else {
      return sslUsed ? sslUnencryptedMetrics : nonSslUnencryptedMetrics;
    }
  }

  /**
   * Get a {@link ContainerMetrics} instance for the specified container for this operation type. If a request of this
   * operation type has not been made for the specified container since the frontend was started, new metrics will be
   * created.
   * @param accountName the account name.
   * @param containerName the container name.
   * @return the {@link ContainerMetrics} instance, or {@code null} if container metrics are disabled for this type of
   *         operation.
   */
  ContainerMetrics getContainerMetrics(String accountName, String containerName) {
    return containerMetricsEnabled ? instantiatedContainerMetrics.computeIfAbsent(
        new Pair<>(accountName, containerName),
        k -> new ContainerMetrics(accountName, containerName, requestType, metricRegistry)) : null;
  }
}
