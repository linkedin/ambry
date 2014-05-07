package com.github.ambry.coordinator;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Metrics for the coordinator
 */
public class CoordinatorMetrics {
  public final Histogram putBlobOperationLatencyInMs;
  public final Histogram deleteBlobOperationLatencyInMs;
  public final Histogram getBlobPropertiesOperationLatencyInMs;
  public final Histogram getBlobUserMetadataOperationLatencyInMs;
  public final Histogram getBlobOperationLatencyInMs;

  public final Meter putBlobOperationRate;
  public final Meter deleteBlobOperationRate;
  public final Meter getBlobPropertiesOperationRate;
  public final Meter getBlobUserMetadataOperationRate;
  public final Meter getBlobOperationRate;
  public final Meter operationExceptionRate;

  public final Counter unexpectedInternalError;
  public final Counter ambryUnavailableError;
  public final Counter operationTimedOutError;
  public final Counter invalidBlobIdError;
  public final Counter invalidPutArgumentError;
  public final Counter insufficientCapacityError;
  public final Counter blobTooLargeError;
  public final Counter blobDoesNotExistError;
  public final Counter blobDeletedError;
  public final Counter blobExpiredError;
  public final Counter unknownError;

  private final Map<DataNodeId, RequestMetrics> requestMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public CoordinatorMetrics(ClusterMap clusterMap) {
    MetricRegistry registry = clusterMap.getMetricRegistry();
    putBlobOperationLatencyInMs =
            registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "putBlobOperationLatencyInMs"));
    deleteBlobOperationLatencyInMs =
            registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobOperationLatencyInMs"));
    getBlobPropertiesOperationLatencyInMs =
            registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesOperationLatencyInMs"));
    getBlobUserMetadataOperationLatencyInMs =
            registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataOperationLatencyInMs"));
    getBlobOperationLatencyInMs =
            registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "getBlobOperationLatencyInMs"));

    putBlobOperationRate =
            registry.meter(MetricRegistry.name(AmbryCoordinator.class, "putBlobOperationRate"));
    deleteBlobOperationRate =
            registry.meter(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobOperationRate"));
    getBlobPropertiesOperationRate =
            registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesOperationRate"));
    getBlobUserMetadataOperationRate =
            registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataOperationRate"));
    getBlobOperationRate =
            registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobOperationRate"));
    operationExceptionRate =
            registry.meter(MetricRegistry.name(AmbryCoordinator.class, "operationExceptionRate"));

    unexpectedInternalError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unexpectedInternalError"));
    ambryUnavailableError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "ambryUnavailableError"));
    operationTimedOutError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "operationTimedOutError"));
    invalidBlobIdError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "invalidBlobIdError"));
    invalidPutArgumentError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "invalidPutArgumentError"));
    insufficientCapacityError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "insufficientCapacityError"));
    blobTooLargeError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobTooLargeError"));
    blobDoesNotExistError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobDoesNotExistError"));
    blobDeletedError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobDeletedError"));
    blobExpiredError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobExpiredError"));
    unknownError =
            registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unknownError"));

    // Track metrics at DataNode granularity.
    // In the future, could track at Disk and/or Partition granularity as well/instead.
    requestMetrics = new HashMap<DataNodeId, RequestMetrics>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
        requestMetrics.put(dataNodeId, new RequestMetrics(registry, dataNodeId));
    }
  }

  public void countError(CoordinatorError error) {
    operationExceptionRate.mark();
    switch (error) {
      case UnexpectedInternalError:
        unexpectedInternalError.inc();
        break;
      case AmbryUnavailable:
        ambryUnavailableError.inc();
        break;
      case OperationTimedOut:
        operationTimedOutError.inc();
        break;
      case InvalidBlobId:
        invalidBlobIdError.inc();
        break;
      case InvalidPutArgument:
        invalidPutArgumentError.inc();
        break;
      case InsufficientCapacity:
        insufficientCapacityError.inc();
        break;
      case BlobTooLarge:
        blobTooLargeError.inc();
        break;
      case BlobDoesNotExist:
        blobDoesNotExistError.inc();
        break;
      case BlobDeleted:
        blobDeletedError.inc();
        break;
      case BlobExpired:
        blobExpiredError.inc();
        break;
      default:
        logger.warn("Unknown CoordinatorError being counted: " + error);
        unknownError.inc();
        break;
    }
  }

  public RequestMetrics getRequestMetrics(DataNodeId dataNodeId) throws CoordinatorException {
    if (requestMetrics.containsKey(dataNodeId)) {
      return requestMetrics.get(dataNodeId);
    }
    else {
      throw new CoordinatorException("Could not find RequestMetrics for DataNode " + dataNodeId,
                                     CoordinatorError.UnexpectedInternalError);
    }
  }

  public class RequestMetrics {
    public final Histogram putBlobRequestLatencyInMs;
    public final Histogram deleteBlobRequestLatencyInMs;
    public final Histogram getBlobPropertiesRequestLatencyInMs;
    public final Histogram getBlobUserMetadataRequestLatencyInMs;
    public final Histogram getBlobRequestLatencyInMs;

    public final Meter putBlobRequestRate;
    public final Meter deleteBlobRequestRate;
    public final Meter getBlobPropertiesRequestRate;
    public final Meter getBlobUserMetadataRequestRate;
    public final Meter getBlobRequestRate;
    public final Meter requestErrorRate;

    public final Counter unexpectedError;
    public final Counter ioError;
    public final Counter messageFormatError;
    public final Counter timeoutError;
    public final Counter unknownError;

    RequestMetrics(MetricRegistry registry, DataNodeId dataNodeId) {
      putBlobRequestLatencyInMs =
              registry.histogram(MetricRegistry.name(OperationRequest.class,
                                                     dataNodeId.getDatacenterName(),
                                                     dataNodeId.getHostname(),
                                                     Integer.toString(dataNodeId.getPort()),
                                                     "putBlobRequestLatencyInMs"));
      deleteBlobRequestLatencyInMs =
              registry.histogram(MetricRegistry.name(OperationRequest.class,
                                                     dataNodeId.getDatacenterName(),
                                                     dataNodeId.getHostname(),
                                                     Integer.toString(dataNodeId.getPort()),
                                                     "deleteBlobRequestLatencyInMs"));
      getBlobPropertiesRequestLatencyInMs =
              registry.histogram(MetricRegistry.name(OperationRequest.class,
                                                     dataNodeId.getDatacenterName(),
                                                     dataNodeId.getHostname(),
                                                     Integer.toString(dataNodeId.getPort()),
                                                     "getBlobPropertiesRequestLatencyInMs"));
      getBlobUserMetadataRequestLatencyInMs =
              registry.histogram(MetricRegistry.name(OperationRequest.class,
                                                     dataNodeId.getDatacenterName(),
                                                     dataNodeId.getHostname(),
                                                     Integer.toString(dataNodeId.getPort()),
                                                     "getBlobUserMetadataRequestLatencyInMs"));
      getBlobRequestLatencyInMs =
              registry.histogram(MetricRegistry.name(OperationRequest.class,
                                                     dataNodeId.getDatacenterName(),
                                                     dataNodeId.getHostname(),
                                                     Integer.toString(dataNodeId.getPort()),
                                                     "getBlobRequestLatencyInMs"));

      putBlobRequestRate =
              registry.meter(MetricRegistry.name(OperationRequest.class,
                                                 dataNodeId.getDatacenterName(),
                                                 dataNodeId.getHostname(),
                                                 Integer.toString(dataNodeId.getPort()),
                                                 "putBlobRequestRate"));
      deleteBlobRequestRate =
              registry.meter(MetricRegistry.name(OperationRequest.class,
                                                 dataNodeId.getDatacenterName(),
                                                 dataNodeId.getHostname(),
                                                 Integer.toString(dataNodeId.getPort()),
                                                 "deleteBlobRequestRate"));
      getBlobPropertiesRequestRate =
              registry.meter(MetricRegistry.name(OperationRequest.class,
                                                 dataNodeId.getDatacenterName(),
                                                 dataNodeId.getHostname(),
                                                 Integer.toString(dataNodeId.getPort()),
                                                 "getBlobPropertiesRequestRate"));
      getBlobUserMetadataRequestRate =
              registry.meter(MetricRegistry.name(OperationRequest.class,
                                                 dataNodeId.getDatacenterName(),
                                                 dataNodeId.getHostname(),
                                                 Integer.toString(dataNodeId.getPort()),
                                                 "getBlobUserMetadataRequestRate"));
      getBlobRequestRate =
              registry.meter(MetricRegistry.name(OperationRequest.class,
                                                 dataNodeId.getDatacenterName(),
                                                 dataNodeId.getHostname(),
                                                 Integer.toString(dataNodeId.getPort()),
                                                 "getBlobRequestRate"));
      requestErrorRate =
              registry.meter(MetricRegistry.name(AmbryCoordinator.class,
                                                 dataNodeId.getDatacenterName(),
                                                 dataNodeId.getHostname(),
                                                 Integer.toString(dataNodeId.getPort()),
                                                 "requestErrorRate"));

      unexpectedError =
              registry.counter(MetricRegistry.name(AmbryCoordinator.class,
                                                   dataNodeId.getDatacenterName(),
                                                   dataNodeId.getHostname(),
                                                   Integer.toString(dataNodeId.getPort()),
                                                   "unexpectedError"));
      ioError =
              registry.counter(MetricRegistry.name(AmbryCoordinator.class,
                                                   dataNodeId.getDatacenterName(),
                                                   dataNodeId.getHostname(),
                                                   Integer.toString(dataNodeId.getPort()),
                                                   "ioError"));
      messageFormatError =
              registry.counter(MetricRegistry.name(AmbryCoordinator.class,
                                                   dataNodeId.getDatacenterName(),
                                                   dataNodeId.getHostname(),
                                                   Integer.toString(dataNodeId.getPort()),
                                                   "messageFormatError"));
      timeoutError =
              registry.counter(MetricRegistry.name(AmbryCoordinator.class,
                                                   dataNodeId.getDatacenterName(),
                                                   dataNodeId.getHostname(),
                                                   Integer.toString(dataNodeId.getPort()),
                                                   "timeoutError"));
      unknownError =
              registry.counter(MetricRegistry.name(AmbryCoordinator.class,
                                                   dataNodeId.getDatacenterName(),
                                                   dataNodeId.getHostname(),
                                                   Integer.toString(dataNodeId.getPort()),
                                                   "unknownError"));
    }

    public void countError(RequestResponseError error) {
      requestErrorRate.mark();
      switch (error) {
        case UNEXPECTED_ERROR:
          unexpectedError.inc();
          break;
        case IO_ERROR:
          ioError.inc();
          break;
        case MESSAGE_FORMAT_ERROR:
          messageFormatError.inc();
          break;
        case TIMEOUT_ERROR:
          timeoutError.inc();
          break;
        default:
          logger.warn("Unknown RequestResponseError being counted: " + error);
          unknownError.inc();
          break;
      }
    }
  }
}
