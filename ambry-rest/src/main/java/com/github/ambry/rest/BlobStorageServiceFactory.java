package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.utils.Utils;


/**
 * TODO: write description
 */
public class BlobStorageServiceFactory {
  public static String BLOBSTORAGE_SERVICE_CLASS_KEY = "rest.blobstorage.service";

  public static BlobStorageService getBlobStorageService(VerifiableProperties verifiableProperties,
      ClusterMap clusterMap, MetricRegistry metricRegistry)
      throws Exception {
    String blobStorageServiceClassName = verifiableProperties.getString(BLOBSTORAGE_SERVICE_CLASS_KEY);
    return Utils.getObj(blobStorageServiceClassName, verifiableProperties, clusterMap, metricRegistry);
  }
}
