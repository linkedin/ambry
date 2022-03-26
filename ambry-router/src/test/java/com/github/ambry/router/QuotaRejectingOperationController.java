/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link OperationController} implementation, that rejects all requests for quota compliance.
 */
public class QuotaRejectingOperationController extends OperationController {
  private static final Logger logger = LoggerFactory.getLogger(QuotaRejectingOperationController.class);
  public QuotaRejectingOperationController(String suffix, String defaultPartitionClass, AccountService accountService,
      NetworkClientFactory networkClientFactory, ClusterMap clusterMap, RouterConfig routerConfig,
      ResponseHandler responseHandler, NotificationSystem notificationSystem, NonBlockingRouterMetrics routerMetrics,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time,
      NonBlockingRouter nonBlockingRouter) throws IOException {
    super(suffix, defaultPartitionClass, accountService, networkClientFactory, clusterMap, routerConfig,
        responseHandler, notificationSystem, routerMetrics, kms, cryptoService, cryptoJobHandler, time,
        nonBlockingRouter);
  }

  @Override
  public void run() {
    try {
      while (nonBlockingRouter.isOpen.get()) {
        List<RequestInfo> requestsToSend = new ArrayList<>();
        Set<Integer> requestsToDrop = new HashSet<>();
        pollForRequests(requestsToSend, requestsToDrop);

        List<ResponseInfo> responseInfoList = new ArrayList<>();
        for(RequestInfo requestInfo : requestsToSend) {
          responseInfoList.add(new ResponseInfo(requestInfo, true));
        }
        onResponse(responseInfoList);
        responseInfoList.forEach(ResponseInfo::release);
      }
    } catch (Throwable e) {
      logger.error("Aborting, as requestResponseHandlerThread received an unexpected error: ", e);
      routerMetrics.requestResponseHandlerUnexpectedErrorCount.inc();
    } finally {
      // Close the router.
      nonBlockingRouter.shutDownOperationControllers();
    }
  }
}
