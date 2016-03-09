package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;


/**
 * {@link InMemoryRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link InMemoryRouter} and returns a new instance on
 * {@link #getRouter()}.
 */
public class InMemoryRouterFactory implements RouterFactory {
  private final VerifiableProperties verifiableProperties;
  private final NotificationSystem notificationSystem;

  public InMemoryRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) {
    this.verifiableProperties = verifiableProperties;
    this.notificationSystem = notificationSystem;
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    return new InMemoryRouter(verifiableProperties, notificationSystem);
  }
}
