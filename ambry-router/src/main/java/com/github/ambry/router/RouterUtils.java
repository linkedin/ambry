/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.router;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.LocalNetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.Response;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a utility class used by Router.
 */
public class RouterUtils {

  private static final Logger logger = LoggerFactory.getLogger(RouterUtils.class);

  /**
   * Get {@link BlobId} from a blob string.
   * @param blobIdString The string of blobId.
   * @param clusterMap The {@link ClusterMap} based on which to generate the {@link BlobId}.
   * @return BlobId
   * @throws RouterException If parsing a string blobId fails.
   */
  static BlobId getBlobIdFromString(String blobIdString, ClusterMap clusterMap) throws RouterException {
    BlobId blobId;
    try {
      blobId = new BlobId(blobIdString, clusterMap);
      logger.trace("BlobId {} created with partitionId {}", blobId, blobId.getPartition());
    } catch (Exception e) {
      logger.trace("Caller passed in invalid BlobId {}", blobIdString);
      throw new RouterException("BlobId is invalid " + blobIdString, RouterErrorCode.InvalidBlobId);
    }
    return blobId;
  }

  /**
   * Checks if the given {@link ReplicaId} is in a different data center relative to a router with the
   * given {@link RouterConfig}
   * @param routerConfig the {@link RouterConfig} associated with a router
   * @param replicaId the {@link ReplicaId} whose status (local/remote) is to be determined.
   * @return true if the replica is remote, false otherwise.
   */
  static boolean isRemoteReplica(RouterConfig routerConfig, ReplicaId replicaId) {
    return !routerConfig.routerDatacenterName.equals(replicaId.getDataNodeId().getDatacenterName());
  }

  /**
   * Determine if an error is indicative of the health of the system, and not a user error.
   * If it is a system health error, then the error is logged.
   * @param exception The {@link Exception} to check.
   * @return true if this is an internal error and not a user error; false otherwise.
   */
  static boolean isSystemHealthError(Exception exception) {
    boolean isSystemHealthError = true;
    boolean isInternalError = false;
    if (exception instanceof RouterException) {
      RouterErrorCode routerErrorCode = ((RouterException) exception).getErrorCode();
      switch (routerErrorCode) {
        // The following are user errors. Only increment the respective error metric.
        case InvalidBlobId:
        case InvalidPutArgument:
        case BlobTooLarge:
        case BadInputChannel:
        case BlobDeleted:
        case BlobExpired:
        case BlobAuthorizationFailure:
        case BlobDoesNotExist:
        case RangeNotSatisfiable:
        case ChannelClosed:
        case BlobUpdateNotAllowed:
          isSystemHealthError = false;
          break;
        case UnexpectedInternalError:
          isInternalError = true;
          break;
      }
    } else if (Utils.isPossibleClientTermination(exception)) {
      isSystemHealthError = false;
    }
    if (isSystemHealthError) {
      if (isInternalError) {
        logger.error("Router operation met with a system health error: ", exception);
      } else {
        // Be less verbose with transient errors like operation timeouts
        logger.warn("Router operation error: {}", exception.toString());
      }
    }
    return isSystemHealthError;
  }

  /**
   * Return the number of data chunks for the given blob and chunk sizes.
   * @param blobSize the size of the overall blob.
   * @param chunkSize the size of each data chunk (except, possibly the last one).
   * @return the number of data chunks for the given blob and chunk sizes.
   */
  static int getNumChunksForBlobAndChunkSize(long blobSize, int chunkSize) {
    return (int) (blobSize == 0 ? 1 : (blobSize - 1) / chunkSize + 1);
  }

  /**
   * Return {@link Account} and {@link Container} in a {@link Pair}.
   * @param accountService the accountService to translate accountId to name.
   * @param accountId the accountId to translate.
   * @return {@link Account} and {@link Container} in a {@link Pair}.
   */
  static Pair<Account, Container> getAccountContainer(AccountService accountService, short accountId,
      short containerId) {
    Account account = accountService.getAccountById(accountId);
    Container container = account == null ? null : account.getContainerById(containerId);
    return new Pair<>(account, container);
  }

  /**
   * @param correlationId correlation ID for the request that timed out
   * @param dataNode the node that the request was made to.
   * @param blobId the blob ID of the request.
   * @return a {@link RouterException} with the {@link RouterErrorCode#OperationTimedOut} error code.
   */
  static RouterException buildTimeoutException(int correlationId, DataNodeId dataNode, BlobId blobId) {
    return new RouterException(
        "Timed out waiting for a response. correlationId=" + correlationId + ", dataNode=" + dataNode + ", blobId="
            + blobId, RouterErrorCode.OperationTimedOut);
  }

  /**
   * Atomically replace the exception for an operation depending on the precedence of the new exception.
   * First, if the current operationException is null, directly set operationException as exception;
   * Second, if operationException exists, compare ErrorCodes of exception and existing operation Exception depending
   * on precedence level. An ErrorCode with a smaller precedence level overrides an ErrorCode with a larger precedence
   * level. Update the operationException if necessary.
   * @param operationExceptionRef the {@link AtomicReference} to the operation exception to potentially replace.
   * @param exception the new {@link RouterException} to set if it the precedence level is lower than that of the
   *                  current exception.
   * @param precedenceLevelFn a function that translates a {@link RouterErrorCode} into an integer precedence level,
   *                          where lower values signify greater precedence.
   */
  static void replaceOperationException(AtomicReference<Exception> operationExceptionRef, RouterException exception,
      ToIntFunction<RouterErrorCode> precedenceLevelFn) {
    operationExceptionRef.updateAndGet(currentException -> {
      Exception newException;
      if (currentException == null) {
        newException = exception;
      } else {
        int currentPrecedence = precedenceLevelFn.applyAsInt(
            currentException instanceof RouterException ? ((RouterException) currentException).getErrorCode()
                : RouterErrorCode.UnexpectedInternalError);
        newException =
            precedenceLevelFn.applyAsInt(exception.getErrorCode()) < currentPrecedence ? exception : currentException;
      }
      return newException;
    });
  }

  /**
   * Extract the {@link Response} from the given {@link ResponseInfo}
   * @param <R> the {@link Response} type.
   * @param responseHandler the {@link ResponseHandler} instance to use.
   * @param routerMetrics the {@link NonBlockingRouterMetrics} instance to use.
   * @param responseInfo the {@link ResponseInfo} from which the {@link Response} is to be extracted.
   * @param deserializer the {@link Deserializer} to use.
   * @param errorExtractor extract the {@link ServerErrorCode} to send to {@link ResponseHandler#onEvent}.
   * @return the extracted {@link Response} if there is one; null otherwise.
   */
  @SuppressWarnings("unchecked")
  static <R extends Response> R extractResponseAndNotifyResponseHandler(ResponseHandler responseHandler,
      NonBlockingRouterMetrics routerMetrics, ResponseInfo responseInfo, Deserializer<R> deserializer,
      Function<R, ServerErrorCode> errorExtractor) {
    R response = null;
    if (responseInfo.isQuotaRejected()) {
      return response;
    }
    ReplicaId replicaId = responseInfo.getRequestInfo().getReplicaId();
    NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
    if (networkClientErrorCode == null) {
      try {
        if (responseInfo.getResponse() != null) {
          // If this responseInfo already has the deserialized java object, we can reference it directly. This is
          // applicable when we are receiving responses from Azure APIs in frontend. The responses from Azure are
          // handled in {@code AmbryRequest} class methods using a thread pool running with in the frontend. These
          // responses are then sent using local queues in {@code LocalRequestResponseChannel}.
          response = (R) mapToReceivedResponse((Response) responseInfo.getResponse());
        } else {
          DataInputStream dis = new NettyByteBufDataInputStream(responseInfo.content());
          response = deserializer.readFrom(dis);
        }
        responseHandler.onEvent(replicaId, errorExtractor.apply(response));
      } catch (Exception e) {
        // Ignore. There is no value in notifying the response handler.
        logger.error("Response deserialization received unexpected error", e);
        routerMetrics.responseDeserializationErrorCount.inc();
      }
    } else {
      responseHandler.onEvent(replicaId, networkClientErrorCode);
    }
    return response;
  }

  /**
   * A temporary method to get port before http2 based replication is implemented.
   * TODO: remove this once http2 based replication is implemented.
   * @param replicaId The {@link ReplicaId} to connect to.
   * @param routerEnableHttp2NetworkClient  if http2 network client should be enabled.
   * @return the port to connect.
   */
  static Port getPortToConnectTo(ReplicaId replicaId, boolean routerEnableHttp2NetworkClient) {
    if (routerEnableHttp2NetworkClient) {
      return new Port(replicaId.getDataNodeId().getHttp2Port(), PortType.HTTP2);
    } else {
      return replicaId.getDataNodeId().getPortToConnectTo();
    }
  }

  /**
   * Used to deserialize an object from a {@link DataInputStream}.
   * @param <T> the type of the deserialized object.
   */
  @FunctionalInterface
  interface Deserializer<T> {
    /**
     * @param stream the {@link DataInputStream} to read from.
     * @return the deserialized object.
     * @throws IOException on deserialization errors.
     */
    T readFrom(DataInputStream stream) throws IOException;
  }

  /**
   * This method is applicable when we are processing responses received from Azure APIs in Frontend via {@link LocalNetworkClient}.
   * The responses received from Azure are constructed as java objects such as {@link GetResponse}, {@link PutResponse} in
   * {@link com.github.ambry.protocol.AmbryRequests} class methods from {@link com.github.ambry.protocol.RequestHandler}
   * threads running within the Frontend itself. The content in these responses is available as buffer but we access it
   * as stream in the Frontend router. Hence, we create new Response objects by having a stream enclose the buffer.
   *
   * At the moment, only {@link GetResponse} carries content and needs to be constructed again as below. Other responses
   * like {@link PutResponse}, {@link DeleteResponse}, etc which don't carry any content don't need to be reconstructed
   * and can be referenced as they are.
   * @param sentResponse {@link Response} object constructed at sender side.
   * @return {@link Response} object constructed at receiver side.
   */
  public static Response mapToReceivedResponse(Response sentResponse) {
    Response receivedResponse;
    if (sentResponse instanceof GetResponse) {
      GetResponse getResponse = (GetResponse) sentResponse;
      receivedResponse = new GetResponse(getResponse.getCorrelationId(), getResponse.getClientId(),
          getResponse.getPartitionResponseInfoList(), new ByteBufInputStream(getResponse.getDataToSend().content()),
          getResponse.getError());
    } else {
      receivedResponse = sentResponse;
    }
    return receivedResponse;
  }

  /**
   * Checks if the request has expired due to either no response from server or it being stuck in router itself
   * (unavailable quota, etc.) for a long time.
   * @param requestInfo of the request.
   * @param currentTimeInMs current time in msec.
   * @return RouterRequestExpiryReason representing the reason for request expiry.
   */
  public static RouterRequestExpiryReason isRequestExpired(RequestInfo requestInfo, long currentTimeInMs) {
    if ((requestInfo.isRequestReceivedByNetworkLayer()
        && currentTimeInMs - requestInfo.getRequestEnqueueTime() > requestInfo.getNetworkTimeOutMs())) {
      return RouterRequestExpiryReason.ROUTER_SERVER_NETWORK_CLIENT_TIMEOUT;
    } else if (currentTimeInMs - requestInfo.getRequestCreateTime() > requestInfo.getFinalTimeOutMs()) {
      return RouterRequestExpiryReason.ROUTER_REQUEST_TIMEOUT;
    }
    return RouterRequestExpiryReason.NO_TIMEOUT;
  }

  /**
   * Update the metrics related to request time out.
   * @param routerRequestExpiryReason the reason for request expiry.
   * @param routerMetrics router metrics.
   * @param requestInfo of the request.
   */
  public static void logTimeoutMetrics(RouterRequestExpiryReason routerRequestExpiryReason,
      NonBlockingRouterMetrics routerMetrics, RequestInfo requestInfo) {
    if (routerRequestExpiryReason == RouterUtils.RouterRequestExpiryReason.ROUTER_SERVER_NETWORK_CLIENT_TIMEOUT) {
      routerMetrics.requestWaitTimeBeforeExpiryOnNetworkTimeoutMs.update(requestInfo.getNetworkTimeOutMs());
      routerMetrics.requestExpiryOnNetworkTimeoutCount.inc();
    } else if (routerRequestExpiryReason == RouterUtils.RouterRequestExpiryReason.ROUTER_REQUEST_TIMEOUT) {
      routerMetrics.requestExpiryOnFinalTimeoutCount.inc();
    }
  }

  /**
   * @param blobId {@link BlobId} to check.
   * @param clusterMap {@link ClusterMap} object.
   * @return {@code true} if it can be determined that the blobid's originating dc is remote. {@code false} otherwise.
   */
  public static boolean isOriginatingDcRemote(BlobId blobId, ClusterMap clusterMap) {
    return blobId.getDatacenterId() != ClusterMap.UNKNOWN_DATACENTER_ID
        && blobId.getDatacenterId() != clusterMap.getLocalDatacenterId();
  }

  /**
   * {@link Enum} All the reasons for router request expiry.
   */
  public enum RouterRequestExpiryReason {
    /**
     * No timeout.
     */
    NO_TIMEOUT,
    /**
     * Network timeout between router and server.
     */
    ROUTER_SERVER_NETWORK_CLIENT_TIMEOUT,
    /**
     * Request timed out in the router.
     */
    ROUTER_REQUEST_TIMEOUT
  }
}
