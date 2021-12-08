/**
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
 */
package com.github.ambry.router;

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link OperationController} implementation that dispatches chunk requests to server based on quota.
 */
public class QuotaAwareOperationController extends OperationController {
  private static final Logger logger = LoggerFactory.getLogger(QuotaAwareOperationController.class);
  // QuotaResource instance to use for cases where OperationController gets null QuotaResource.
  private static final QuotaResource UNKNOWN_QUOTA_RESOURCE = new QuotaResource("UNKNOWN", QuotaResourceType.ACCOUNT);
  private final Map<QuotaResource, LinkedList<RequestInfo>> readRequestQueue = new HashMap<>();
  private final Map<QuotaResource, LinkedList<RequestInfo>> writeRequestQueue = new HashMap<>();

  /**
   * Constructor for {@link QuotaAwareOperationController} class.
   * @param suffix the suffix to associate with the thread names of this OperationController
   * @param defaultPartitionClass the default partition class to choose partitions from (if none is found in the
   *                              container config). Can be {@code null} if no affinity is required for the puts for
   *                              which the container contains no partition class hints.
   * @param accountService the {@link AccountService} to use.
   * @param networkClientFactory the {@link NetworkClientFactory} used by the {@link OperationController} to create
   *                             instances of {@link NetworkClient}.
   * @param clusterMap the cluster map for the cluster.
   * @param routerConfig the configs for the router.
   * @param responseHandler {@link ResponseHandler} object.
   * @param notificationSystem the notification system to use to notify about blob creations and deletions.
   * @param routerMetrics the metrics for the router.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the time instance.
   * @param nonBlockingRouter {@link NonBlockingRouter} object.
   * @throws IOException if the network components could not be created.
   */
  public QuotaAwareOperationController(String suffix, String defaultPartitionClass, AccountService accountService,
      NetworkClientFactory networkClientFactory, ClusterMap clusterMap, RouterConfig routerConfig,
      ResponseHandler responseHandler, NotificationSystem notificationSystem, NonBlockingRouterMetrics routerMetrics,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time,
      NonBlockingRouter nonBlockingRouter) throws IOException {
    super(suffix, defaultPartitionClass, accountService, networkClientFactory, clusterMap, routerConfig,
        responseHandler, notificationSystem, routerMetrics, kms, cryptoService, cryptoJobHandler, time,
        nonBlockingRouter);
  }

  @Override
  protected void pollForRequests(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    try {
      List<RequestInfo> newRequestsToSend = new ArrayList<>();
      pollNewRequests(newRequestsToSend, requestsToDrop);
      addToRequestQueue(newRequestsToSend);
      drainRequestQueue(requestsToSend);
    } catch (Exception e) {
      logger.error("Operation Manager poll received an unexpected error: ", e);
      routerMetrics.operationManagerPollErrorCount.inc();
    }
  }

  /**
   * Poll for new requests from each of the operation managers, and initiate background deletes.
   * @param requestsToSend a list of {@link RequestInfo} that will contain the new requests to be sent out.
   * @param requestsToDrop a list of correlation IDs that will contain the IDs for requests that the network layer
   *                       should drop.
   */
  private void pollNewRequests(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    putManager.poll(requestsToSend, requestsToDrop);
    getManager.poll(requestsToSend, requestsToDrop);
    nonBlockingRouter.initiateBackgroundDeletes(getBackGroundDeleteRequests());
    clearBackGroundDeleteRequests();
    deleteManager.poll(requestsToSend, requestsToDrop);
    ttlUpdateManager.poll(requestsToSend, requestsToDrop);
    undeleteManager.poll(requestsToSend, requestsToDrop);
  }

  /**
   * Add the specified {@link List} of {@link RequestInfo}s to the request queue.
   * @param requestInfos {@link List} of {@link RequestInfo} objects.
   */
  private void addToRequestQueue(List<RequestInfo> requestInfos) {
    for (RequestInfo requestInfo : requestInfos) {
      QuotaResource quotaResource = requestInfo.getChargeable().getQuotaResource();
      if (quotaResource == null) {
        quotaResource = UNKNOWN_QUOTA_RESOURCE;
      }
      getRequestQueue(requestInfo.getChargeable().getQuotaMethod()).putIfAbsent(quotaResource, new LinkedList<>());
      getRequestQueue(requestInfo.getChargeable().getQuotaMethod()).get(quotaResource).add(requestInfo);
    }
  }

  /**
   * Drain the request queue based on quota and system resources and update the requests to be send to {@code requestsToSend}.
   * @param requestsToSend a list of {@link RequestInfo} that will contain the requests to be sent out.
   */
  private void drainRequestQueue(List<RequestInfo> requestsToSend) {
    for (QuotaMethod quotaMethod : QuotaMethod.values()) {
      pollQuotaCompliantRequests(requestsToSend, getRequestQueue(quotaMethod));
      pollQuotaExceedAllowedRequestsIfAny(requestsToSend, getRequestQueue(quotaMethod));
    }
  }

  /**
   * Poll for out of quota requests that are allowed to exceed quota.
   * @param requestsToSend {@link List} of {@link RequestInfo} to be sent.
   * @param requestQueue {@link Map} of {@link QuotaResource} to {@link List} of {@link RequestInfo} from which the requests to be sent will be polled.
   */
  private void pollQuotaExceedAllowedRequestsIfAny(List<RequestInfo> requestsToSend,
      Map<QuotaResource, LinkedList<RequestInfo>> requestQueue) {
    List<QuotaResource> quotaResources = new ArrayList<>(requestQueue.keySet());
    Collections.shuffle(quotaResources);
    while (!requestQueue.isEmpty()) {
      for (QuotaResource quotaResource : quotaResources) {
        RequestInfo requestInfo = requestQueue.get(quotaResource).getFirst();
        if (!requestInfo.getChargeable().quotaExceedAllowed()) {
          // If quota exceeded requests aren't allowed, then there is nothing more to do.
          return;
        }
        requestInfo.getChargeable().charge();
        requestsToSend.add(requestInfo);
        requestQueue.get(quotaResource).removeFirst();
        if (requestQueue.get(quotaResource).isEmpty()) {
          requestQueue.remove(quotaResource);
        }
      }
    }
  }

  /**
   * Drain the request queue based on resource quota only and update the requests to be sent to {@code requestsToSend}.
   * @param requestsToSend a list of {@link RequestInfo} that will contain the requests to be sent out.
   * @param requestQueue {@link Map} of {@link QuotaResource} to {@link List} of {@link RequestInfo} from which the requests to be sent will be polled.
   */
  private void pollQuotaCompliantRequests(List<RequestInfo> requestsToSend,
      Map<QuotaResource, LinkedList<RequestInfo>> requestQueue) {
    for (QuotaResource quotaResource : requestQueue.keySet()) {
      if (quotaResource.equals(UNKNOWN_QUOTA_RESOURCE)) {
        // If there are requests for which QuotaResource couldn't be found, this is most likely a bug.
        // As a temporary hack, we will consider all those requests to be quota compliant.
        requestsToSend.addAll(requestQueue.get(UNKNOWN_QUOTA_RESOURCE));
        requestQueue.remove(UNKNOWN_QUOTA_RESOURCE);
        continue;
      }
      while (!requestQueue.get(quotaResource).isEmpty()) {
        RequestInfo requestInfo = requestQueue.get(quotaResource).getFirst();
        if (requestInfo.getChargeable().check()) {
          requestsToSend.add(requestInfo);
          requestQueue.get(quotaResource).removeFirst();
          requestInfo.getChargeable().charge();
        } else {
          break;
        }
      }
      if (requestQueue.get(quotaResource).isEmpty()) {
        requestQueue.remove(quotaResource);
      }
    }
  }

  /**
   * @param quotaMethod {@link QuotaMethod} of the requested request queue.
   * @return the request queue of the OperationController corresponding to quotaMethod.
   */
  Map<QuotaResource, LinkedList<RequestInfo>> getRequestQueue(QuotaMethod quotaMethod) {
    return quotaMethod == QuotaMethod.READ ? readRequestQueue : writeRequestQueue;
  }
}
