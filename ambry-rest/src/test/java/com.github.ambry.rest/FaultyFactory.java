package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;


public class FaultyFactory implements BlobStorageServiceFactory, NioServerFactory, RouterFactory {

  // for BlobStorageServiceFactory
  public FaultyFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap, Router router) {
    // don't care.
  }

  // for NioServerFactory
  public FaultyFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RequestResponseHandlerController requestResponseHandlerController) {
    // don't care.
  }

  // for RouterFactory
  public FaultyFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) {
    // don't care.
  }

  @Override
  public BlobStorageService getBlobStorageService()
      throws InstantiationException {
    return null;
  }

  @Override
  public NioServer getNioServer()
      throws InstantiationException {
    return null;
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    return null;
  }
}