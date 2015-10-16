package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;


/**
 * TODO: write description
 */
public class InMemoryRouterFactory implements RouterFactory {
  private final VerifiableProperties verifiableProperties;

  public InMemoryRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap, NotificationSystem notificationSystem) {
    this.verifiableProperties = verifiableProperties;
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    return new InMemoryRouter(verifiableProperties);
  }
}
