package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link NonBlockingRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NonBlockingRouter} and returns a new
 * instance on {@link #getRouter()}.
 */
public class NonBlockingRouterFactory implements RouterFactory {
  private final RouterConfig routerConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final MetricRegistry registry;
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates an instance of NonBlockingRouterFactory with the given {@code verifiableProperties},
   * {@code clusterMap} and {@code notificationSystem}.
   * @param verifiableProperties the in-memory properties to use to construct configurations.
   * @param clusterMap the {@link ClusterMap} to use to determine where operations should go.
   * @param notificationSystem the {@link NotificationSystem} to use to log operations.
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public NonBlockingRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) {
    if (verifiableProperties != null && clusterMap != null && notificationSystem != null) {
      routerConfig = new RouterConfig(verifiableProperties);
      registry = clusterMap.getMetricRegistry();
      routerMetrics = new NonBlockingRouterMetrics(registry);
      this.clusterMap = clusterMap;
      this.notificationSystem = notificationSystem;
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
    logger.trace("Instantiated NonBlockingRouterFactory");
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    try {
      return new NonBlockingRouter(routerConfig, registry, notificationSystem, clusterMap);
    } catch (IOException e) {
      throw new InstantiationException("Error instantiating NonBlocking Router" + e.getMessage());
    }
  }
}

