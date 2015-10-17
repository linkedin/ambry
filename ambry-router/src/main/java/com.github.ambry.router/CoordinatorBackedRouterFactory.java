package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.notification.NotificationSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: null checks etc.

/**
 * {@link CoordinatorBackedRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link CoordinatorBackedRouter} and returns a new
 * instance on {@link CoordinatorBackedRouterFactory#getRouter()}.
 */
public class CoordinatorBackedRouterFactory implements RouterFactory {
  private final RouterConfig routerConfig;
  private final MetricRegistry metricRegistry;
  private final Coordinator coordinator;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public CoordinatorBackedRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) {
    if (verifiableProperties != null && clusterMap != null && notificationSystem != null) {
      routerConfig = new RouterConfig(verifiableProperties);
      metricRegistry = clusterMap.getMetricRegistry();
      coordinator = new AmbryCoordinator(verifiableProperties, clusterMap, notificationSystem);
    } else {
      StringBuilder errorMessage =
          new StringBuilder("Null arg(s) received during instantiation of CoordinatorBackedRouterFactory -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (clusterMap == null) {
        errorMessage.append(" [ClusterMap] ");
      }
      if (notificationSystem == null) {
        errorMessage.append(" [NotificationSystem] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
    logger.trace("Instantiated CoordinatorBackedRouterFactory");
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    return new CoordinatorBackedRouter(routerConfig, metricRegistry, coordinator);
  }
}
