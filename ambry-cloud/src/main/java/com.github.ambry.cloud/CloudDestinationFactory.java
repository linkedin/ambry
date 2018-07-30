package com.github.ambry.cloud;

// TODO: change this to interface
public class CloudDestinationFactory {

  private static CloudDestinationFactory singleton = new CloudDestinationFactory();

  private CloudDestinationFactory() {
  }

  public static CloudDestinationFactory getInstance() {
    return singleton;
  }

  public CloudDestination getCloudDestination(CloudDestinationType type, String configString) throws Exception {
    CloudDestination destination = null;
    switch (type) {
      case AZURE:
        destination = new AzureCloudDestination();
        break;
        default:
          throw new IllegalArgumentException("Invalid type: " + type);
    }
    destination.initialize(configString);
    return destination;
  }
}
