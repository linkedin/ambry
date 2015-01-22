package com.github.ambry.coordinator;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private final Counter putBlobError;
  private final Counter deleteBlobError;
  private final Counter getBlobPropertiesError;
  private final Counter getBlobUserMetadataError;
  private final Counter getBlobError;

  private final Counter unexpectedInternalError;
  private final Counter ambryUnavailableError;
  private final Counter operationTimedOutError;
  private final Counter invalidBlobIdError;
  private final Counter invalidPutArgumentError;
  private final Counter insufficientCapacityError;
  private final Counter blobTooLargeError;
  private final Counter blobDoesNotExistError;
  private final Counter blobDeletedError;
  private final Counter blobExpiredError;
  private final Counter unknownError;

  public final Counter corruptionError;

  public final Counter unknownReplicaResponseError;
  public final Counter successfulCrossColoProxyCallCount;
  public final Counter totalCrossColoProxyCallCount;

  public final Gauge<Integer> crossColoCallsEnabled;

  private final Map<DataNodeId, RequestMetrics> requestMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public CoordinatorMetrics(ClusterMap clusterMap, final boolean crossDCProxyCallsEnabled) {
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

    putBlobOperationRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "putBlobOperationRate"));
    deleteBlobOperationRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobOperationRate"));
    getBlobPropertiesOperationRate =
        registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesOperationRate"));
    getBlobUserMetadataOperationRate =
        registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataOperationRate"));
    getBlobOperationRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobOperationRate"));
    operationExceptionRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "operationExceptionRate"));

    putBlobError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "putBlobError"));
    deleteBlobError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobError"));
    getBlobPropertiesError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesError"));
    getBlobUserMetadataError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataError"));
    getBlobError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "getBlobError"));

    unexpectedInternalError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unexpectedInternalError"));
    ambryUnavailableError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "ambryUnavailableError"));
    operationTimedOutError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "operationTimedOutError"));
    invalidBlobIdError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "invalidBlobIdError"));
    invalidPutArgumentError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "invalidPutArgumentError"));
    insufficientCapacityError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "insufficientCapacityError"));
    blobTooLargeError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobTooLargeError"));
    blobDoesNotExistError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobDoesNotExistError"));
    blobDeletedError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobDeletedError"));
    blobExpiredError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobExpiredError"));
    unknownError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unknownError"));
    corruptionError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "corruptionError"));
    unknownReplicaResponseError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unknownReplicaResponseError"));
    successfulCrossColoProxyCallCount =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "successfulCrossColoProxyCallCount"));
    totalCrossColoProxyCallCount =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "totalCrossColoProxyCallCount"));
    this.crossColoCallsEnabled = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return (crossDCProxyCallsEnabled == true ? 1 : 0);
      }
    };

    // Track metrics at DataNode granularity.
    // In the future, could track at Disk and/or Partition granularity as well/instead.
    requestMetrics = new HashMap<DataNodeId, RequestMetrics>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      requestMetrics.put(dataNodeId, new RequestMetrics(registry, dataNodeId));
    }
  }

  public enum CoordinatorOperationType {
    PutBlob,
    DeleteBlob,
    GetBlobProperties,
    GetBlobUserMetadata,
    GetBlob
  }

  public void countError(CoordinatorOperationType operation, CoordinatorError error) {
    operationExceptionRate.mark();

    switch (operation) {
      case PutBlob:
        putBlobError.inc();
        break;
      case DeleteBlob:
        deleteBlobError.inc();
        break;
      case GetBlobProperties:
        getBlobPropertiesError.inc();
        break;
      case GetBlobUserMetadata:
        getBlobUserMetadataError.inc();
        break;
      case GetBlob:
        getBlobError.inc();
        break;
      default:
        logger.warn("Error for unknown CoordinatorOperationType being counted: " + operation);
        unknownError.inc();
        break;
    }

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

  public RequestMetrics getRequestMetrics(DataNodeId dataNodeId)
      throws CoordinatorException {
    if (requestMetrics.containsKey(dataNodeId)) {
      return requestMetrics.get(dataNodeId);
    } else {
      String message = "Could not find RequestMetrics for DataNode " + dataNodeId;
      logger.error(message);
      throw new CoordinatorException(message, CoordinatorError.UnexpectedInternalError);
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

    private final Counter unexpectedError;
    private final Counter ioError;
    private final Counter timeoutError;
    private final Counter unknownError;
    private final Counter messageFormatDataCorruptError;
    private final Counter messageFormatHeaderConstraintError;
    private final Counter messageFormatUnknownFormatError;

    RequestMetrics(MetricRegistry registry, DataNodeId dataNodeId) {
      putBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "putBlobRequestLatencyInMs"));
      deleteBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "deleteBlobRequestLatencyInMs"));
      getBlobPropertiesRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobPropertiesRequestLatencyInMs"));
      getBlobUserMetadataRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobUserMetadataRequestLatencyInMs"));
      getBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobRequestLatencyInMs"));

      putBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "putBlobRequestRate"));
      deleteBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "deleteBlobRequestRate"));
      getBlobPropertiesRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobPropertiesRequestRate"));
      getBlobUserMetadataRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobUserMetadataRequestRate"));
      getBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobRequestRate"));
      requestErrorRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "requestErrorRate"));

      unexpectedError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "unexpectedError"));
      ioError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "ioError"));
      timeoutError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "timeoutError"));
      unknownError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "unknownError"));
      messageFormatDataCorruptError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "messageFormatDataCorruptError"));
      messageFormatHeaderConstraintError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "messageFormatHeaderConstraintError"));
      messageFormatUnknownFormatError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "messageFormatUnknownFormatError"));
    }

    public void countError(MessageFormatErrorCodes error) {
      requestErrorRate.mark();
      switch (error) {
        case Data_Corrupt:
          messageFormatDataCorruptError.inc();
          break;
        case Header_Constraint_Error:
          messageFormatHeaderConstraintError.inc();
          break;
        case Unknown_Format_Version:
          messageFormatUnknownFormatError.inc();
          break;
        default:
          logger.warn("Unknown MessageFormatErrorCodes: " + error);
          unknownError.inc();
          break;
      }
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
        case TIMEOUT_ERROR:
          timeoutError.inc();
          break;
        default:
          logger.warn("Unknown RequestResponseError: " + error);
          unknownError.inc();
          break;
      }
    }
  }
}
