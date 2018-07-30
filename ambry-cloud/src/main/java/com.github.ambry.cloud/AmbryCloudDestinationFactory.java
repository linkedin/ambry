package com.github.ambry.cloud;

import com.github.ambry.account.CloudReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AmbryCloudDestinationFactory implements CloudDestinationFactory {

  private VerifiableProperties verifiableProperties;
  private Map<CloudReplicationConfig, CloudDestination> destinationMap;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public AmbryCloudDestinationFactory(VerifiableProperties verifiableProperties) {
    this.verifiableProperties = verifiableProperties;
    // what else does this need?
    destinationMap = new ConcurrentHashMap<>();
  }

  public CloudDestination getCloudDestination(CloudReplicationConfig config) throws InstantiationException {
    CloudDestinationType type = CloudDestinationType.valueOf(config.getDestinationType());

    if (destinationMap.containsKey(config)) {
      return destinationMap.get(config);
    }
    CloudDestination destination = null;
    switch (type) {
      case AZURE:
        destination = new AzureCloudDestination();
        break;
      default:
        throw new IllegalArgumentException("Invalid type: " + type);
    }
    try {
      destination.initialize(config);
    } catch (Exception e) {
      logger.error("Initializing destination with config: " + config);
      throw new InstantiationException(e.getMessage());
    }
    destinationMap.put(config, destination);
    return destination;

  }

}
