package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link NonBlockingRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NonBlockingRouter} and returns a new
 * instance on {@link #getRouter()}.
 */
public class NonBlockingRouterFactory implements RouterFactory {
  protected final RouterConfig routerConfig;
  protected final NonBlockingRouterMetrics routerMetrics;
  protected final ClusterMap clusterMap;
  protected final NetworkConfig networkConfig;
  protected final NetworkMetrics networkMetrics;
  protected final SSLFactory sslFactory;
  protected final NotificationSystem notificationSystem;
  protected final Time time;
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterFactory.class);

  /**
   * Creates an instance of NonBlockingRouterFactory with the given {@code verifiableProperties},
   * {@code clusterMap} and {@code notificationSystem}.
   * @param verifiableProperties the in-memory properties to use to construct configurations.
   * @param clusterMap the {@link ClusterMap} to use to determine where operations should go.
   * @param notificationSystem the {@link NotificationSystem} to use to log operations.
   * @throws IllegalArgumentException if any of the arguments are null.
   * @throws Exception if the SSL configs could not be initialized.
   */
  public NonBlockingRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem)
      throws Exception {
    if (verifiableProperties != null && clusterMap != null && notificationSystem != null) {
      routerConfig = new RouterConfig(verifiableProperties);
      MetricRegistry registry = clusterMap.getMetricRegistry();
      routerMetrics = new NonBlockingRouterMetrics(registry);
      this.clusterMap = clusterMap;
      this.notificationSystem = notificationSystem;
      networkConfig = new NetworkConfig(verifiableProperties);
      networkMetrics = new NetworkMetrics(registry);
      SSLConfig sslConfig = new SSLConfig(verifiableProperties);
      sslFactory = sslConfig.sslEnabledDatacenters.length() > 0 ? new SSLFactory(sslConfig) : null;
      this.time = SystemTime.getInstance();
    } else {
      throw new IllegalArgumentException("Null argument passed in");
    }
    logger.trace("Instantiated NonBlockingRouterFactory");
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    try {
      return new NonBlockingRouter(routerConfig, routerMetrics, networkConfig, networkMetrics, sslFactory,
          notificationSystem, clusterMap, time);
    } catch (Exception e) {
      throw new InstantiationException("Error instantiating NonBlocking Router" + e.getMessage());
    }
  }
}

