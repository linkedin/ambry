package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.utils.Utils;


/**
 * Factory for returning different types of BlobStorageService implementations based on config.
 */
public class BlobStorageServiceFactory {
  public static String BLOBSTORAGE_SERVICE_CLASS_KEY = "rest.blobstorage.service";

  /**
   * Uses a key defined in the configuration file to return the right type of BlobStorageService
   * @param verifiableProperties
   * @param clusterMap
   * @param metricRegistry
   * @return
   * @throws Exception
   */
  public static BlobStorageService getBlobStorageService(VerifiableProperties verifiableProperties,
      ClusterMap clusterMap, MetricRegistry metricRegistry)
      throws Exception {
    String blobStorageServiceClassName = verifiableProperties.getString(BLOBSTORAGE_SERVICE_CLASS_KEY);
    return Utils.getObj(blobStorageServiceClassName, verifiableProperties, clusterMap, metricRegistry);
  }
}
