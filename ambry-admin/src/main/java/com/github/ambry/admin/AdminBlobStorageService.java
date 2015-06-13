package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequest;
import com.github.ambry.restservice.RestResponseHandler;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Admin specific implementation of BlobStorageService. Implements any operations supported by admin.
 */
public class AdminBlobStorageService implements BlobStorageService {
  public static String EXECUTION_DATA_HEADER_KEY = "executionData";

  private final AdminMetrics adminMetrics;
  private final ClusterMap clusterMap;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminBlobStorageService(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      MetricRegistry metricRegistry) {
    this.clusterMap = clusterMap;
    adminMetrics = new AdminMetrics(metricRegistry);
  }

  public void start()
      throws InstantiationException {
    logger.info("Admin blob storage service started");
  }

  public void shutdown()
      throws Exception {
    logger.info("Admin blob storage service shutdown");
  }

  /**
   * Entry point for handling operations. This gets called from the RestMessageHandler.
   * @param messageInfo
   * @throws RestServiceException
   */
  public void handleMessage(MessageInfo messageInfo)
      throws RestServiceException {
    RestMethod restMethod = messageInfo.getRestRequest().getRestMethod();
    switch (restMethod) {
      case GET:
        handleGet(messageInfo);
        break;
      case POST:
        handlePost(messageInfo);
        break;
      case DELETE:
        handleDelete(messageInfo);
        break;
      case HEAD:
        handleHead(messageInfo);
        break;
      default:
        adminMetrics.unknownActionErrorCount.inc();
        throw new RestServiceException("Unknown rest method - " + restMethod, RestServiceErrorCode.UnknownRestMethod);
    }
  }

  /**
   * Extracts the execution data in the header into a friendly object.
   * @param request
   * @return
   * @throws RestServiceException
   */
  private AdminExecutionData extractExecutionData(RestRequest request)
      throws RestServiceException {
    try {
      JSONObject data = new JSONObject(request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY).toString());
      return new AdminExecutionData(data);
    } catch (JSONException e) {
      throw new RestServiceException(EXECUTION_DATA_HEADER_KEY + " not valid JSON - " + e,
          RestServiceErrorCode.BadExecutionData);
    } catch (IllegalArgumentException e) {
      throw new RestServiceException(EXECUTION_DATA_HEADER_KEY + " header does not contain required data - " + e,
          RestServiceErrorCode.BadExecutionData);
    }
  }

  /**
   * Handler for GET operations. Discerns the type of GET and delegates appropriately
   * @param messageInfo
   * @throws RestServiceException
   */
  private void handleGet(MessageInfo messageInfo)
      throws RestServiceException {
    RestRequest request = messageInfo.getRestRequest();
    logger.trace("Handling get request - " + request.getUri());
    if (!isCustomOperation(request)) {
      // TODO: this is a traditional get
      throw new IllegalStateException("Traditional GET not implemented");
    } else {
      handleCustomGetOperation(messageInfo);
    }
  }

  /**
   * Checks for the presence of the executionData header in the request. If the header is present, then
   * a custom operation needs to be executed.
   * @param request
   * @return
   */
  private boolean isCustomOperation(RestRequest request) {
    return request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY) != null;
  }

  /**
   * Handles a custom get operation i.e. an operation that is something other than getting the blob.
   * @param messageInfo
   * @throws RestServiceException
   */
  private void handleCustomGetOperation(MessageInfo messageInfo)
      throws RestServiceException {
    AdminExecutionData executionData = extractExecutionData(messageInfo.getRestRequest());
    AdminOperationType operationType = AdminOperationType.convert(executionData.getOperationType());
    switch (operationType) {
      case Echo:
        handleEcho(messageInfo, executionData);
        break;
      case GetReplicasForBlobId:
        handleGetReplicasForBlobId(messageInfo, executionData);
        break;
      default:
        throw new RestServiceException("Unknown operation type - " + executionData.getOperationType(),
            RestServiceErrorCode.UnknownCustomOperationType);
    }
  }

  /**
   * Handles echo operations. Delegates to a task executor and then writes the response.
   * @param messageInfo
   * @param executionData
   * @throws RestServiceException
   */
  private void handleEcho(MessageInfo messageInfo, AdminExecutionData executionData)
      throws RestServiceException {
    logger.trace("Handling echo");
    try {
      RestResponseHandler responseHandler = messageInfo.getResponseHandler();
      if (messageInfo.getRestObject() instanceof RestRequest) {
        //TODO: Reconsider this model of execution
        TaskExecutor executor = new EchoExecutor();
        String echoStr = executor.execute(executionData).toString();
        if (echoStr != null) {
          responseHandler.setContentType("text/plain");
          responseHandler.finalizeResponse();
          responseHandler.addToBodyAndFlush(echoStr.getBytes(), true);
        } else {
          throw new RestServiceException("Did not get a result for the echo operation",
              RestServiceErrorCode.ResponseBuildingFailure);
        }
      } else {
        responseHandler.close();
      }
    } finally {
      messageInfo.getRestObject().release();
    }
  }

  /**
   * Handles GetReplicaForBlobId operations. Delegates to a task executor and then writes the response.
   * @param messageInfo
   * @param executionData
   * @throws RestServiceException
   */
  private void handleGetReplicasForBlobId(MessageInfo messageInfo, AdminExecutionData executionData)
      throws RestServiceException {
    logger.trace("Handling getReplicas");
    try {
      RestResponseHandler responseHandler = messageInfo.getResponseHandler();
      if (messageInfo.getRestObject() instanceof RestRequest) {
        //TODO: Reconsider this model of execution
        TaskExecutor executor = new GetReplicasForBlobIdExecutor(clusterMap);
        String replicaStr = executor.execute(executionData).toString();
        if (replicaStr != null) {
          responseHandler.setContentType("application/json");
          responseHandler.finalizeResponse();
          responseHandler.addToBodyAndFlush(replicaStr.getBytes(), true);
        } else {
          throw new RestServiceException("Did not get a result for the GetReplicasForBlobId operation",
              RestServiceErrorCode.ResponseBuildingFailure);
        }
      } else {
        responseHandler.close();
      }
    } finally {
      messageInfo.getRestObject().release();
    }
  }

  /**
   * Placeholder for POST operations.
   * @param messageInfo
   */
  private void handlePost(MessageInfo messageInfo) {
    throw new IllegalStateException("handleGet() not implemented in " + this.getClass().getSimpleName());
  }

  /**
   * Placeholder for DELETE operations.
   * @param messageInfo
   */
  private void handleDelete(MessageInfo messageInfo) {
    throw new IllegalStateException("handleDelete() not implemented in " + this.getClass().getSimpleName());
  }

  /**
   * Placeholder for HEAD operations.
   * @param messageInfo
   */
  private void handleHead(MessageInfo messageInfo) {
    throw new IllegalStateException("handleHead() not implemented in " + this.getClass().getSimpleName());
  }
}
