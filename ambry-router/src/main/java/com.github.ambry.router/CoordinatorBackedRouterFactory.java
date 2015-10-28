package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.notification.NotificationSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link CoordinatorBackedRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link CoordinatorBackedRouter} and returns a new
 * instance on {@link #getRouter()}.
 */
public class CoordinatorBackedRouterFactory implements RouterFactory {
  private final RouterConfig routerConfig;
  private final CoordinatorBackedRouterMetrics coordinatorBackedRouterMetrics;
  private final Coordinator coordinator;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates an instance of CoordinatorBackedRouterFactory with the given {@code verifiableProperties},
   * {@code clusterMap} and {@code notificationSystem}.
   * @param verifiableProperties the in-memory properties to use to construct configurations.
   * @param clusterMap the {@link ClusterMap} to use to determine where operations should go.
   * @param notificationSystem the {@link NotificationSystem} to use to log operations.
   */
  public CoordinatorBackedRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) {
    if (verifiableProperties != null && clusterMap != null && notificationSystem != null) {
      routerConfig = new RouterConfig(verifiableProperties);
      coordinatorBackedRouterMetrics = new CoordinatorBackedRouterMetrics(clusterMap.getMetricRegistry());
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
    return new CoordinatorBackedRouter(routerConfig, coordinatorBackedRouterMetrics, coordinator);
  }
}
