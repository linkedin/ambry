package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.notification.NotificationSystem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Streaming, non-blocking router implementation for Ambry.
 */
public class NonBlockingRouter implements Router {
  private final VerifiableProperties properties;
  private final RouterConfig routerConfig;
  private final MetricRegistry metricRegistry;
  private final SSLFactory sslFactory;
  private AtomicReference<ArrayList<OperationController>> ocListRef;
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouter.class);
  static final RouterException ROUTER_CLOSED_EXCEPTION =
      new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed);

  public NonBlockingRouter(VerifiableProperties properties, RouterConfig routerConfig, MetricRegistry metricRegistry,
      NotificationSystem notificationSystem, ClusterMap clusterMap)
      throws Exception {
    this.properties = properties;
    this.routerConfig = routerConfig;
    this.metricRegistry = metricRegistry;
    SSLConfig sslConfig = new SSLConfig(properties);
    if (sslConfig.sslEnabledDatacenters.length() > 0) {
      this.sslFactory = new SSLFactory(sslConfig);
    } else {
      this.sslFactory = null;
    }
    ocListRef = new AtomicReference<ArrayList<OperationController>>(
        new ArrayList<OperationController>(routerConfig.routerScalingUnitCount));
    for (int i = 0; i < routerConfig.routerScalingUnitCount; i++) {
      ocListRef.get().add(new OperationController(this, sslFactory, routerConfig, metricRegistry, notificationSystem,
          clusterMap));
    }
  }

  private OperationController getOperationController() {
    ArrayList<OperationController> ocList = ocListRef.get();
    if (ocList.size() == 0) {
      return null;
    } else {
      return ocList.get(ThreadLocalRandom.current().nextInt(ocList.size()));
    }
  }

  /**
   * Notify that an OperationController is closed.
   * This method is synchronized to ensure concurrent updates are exclusive.
   * @param oc the OperationController that has closed.
   */
  synchronized void onClose(OperationController oc) {
    ArrayList<OperationController> newList = new ArrayList<OperationController>(ocListRef.get());
    newList.remove(oc);
    ocListRef.set(newList);
  }

  /**
   * Requests for the {@link BlobInfo} asynchronously and returns a future that will eventually contain the
   * {@link BlobInfo} on a successful response.
   * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
   * @return A future that would contain the {@link BlobInfo} eventually.
   */
  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.getBlobInfo(blobId, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Requests for the {@link BlobInfo} asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @return A future that would contain the {@link BlobInfo} eventually.
   */
  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, callback, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.getBlobInfo(blobId, futureResult, callback);
    }
    return futureResult;
  }

  /**
   * Requests for blob data asynchronously and returns a future that will eventually contain a
   * {@link ReadableStreamChannel} that represents blob data on a successful response.
   * @param blobId The ID of the blob for which blob data is requested.
   * @return A future that would contain a {@link ReadableStreamChannel} that represents the blob data eventually.
   */
  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId) {
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.getBlob(blobId, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Requests for the blob data asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob for which blob data is requested.
   * @param callback The callback which will be invoked on the completion of the request.
   * @return A future that would contain a {@link ReadableStreamChannel} that represents the blob data eventually.
   */
  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, callback, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.getBlob(blobId, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Requests for a new blob to be put asynchronously and returns a future that will eventually contain the BlobId of
   * the new blob on a successful response.
   * @param blobProperties The properties of the blob.
   * @param usermetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @return A future that would contain the BlobId eventually.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel) {
    FutureResult<String> futureResult = new FutureResult<String>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.putBlob(blobProperties, usermetadata, channel, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Requests for a new blob to be put asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobProperties The properties of the blob.
   * @param usermetadata Optional user metadata about the blob. This can be null.
   * @param channel The {@link ReadableStreamChannel} that contains the content of the blob.
   * @param callback The {@link Callback} which will be invoked on the completion of the request .
   * @return A future that would contain the BlobId eventually.
   */
  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    FutureResult<String> futureResult = new FutureResult<String>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, callback, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.putBlob(blobProperties, usermetadata, channel, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be deleted asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs to be deleted.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId) {
    FutureResult<Void> futureResult = new FutureResult<Void>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, null, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.deleteBlob(blobId, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    FutureResult<Void> futureResult = new FutureResult<Void>();
    OperationController oc = getOperationController();
    if (oc == null) {
      completeOperation(futureResult, null, callback, ROUTER_CLOSED_EXCEPTION);
    } else {
      oc.deleteBlob(blobId, futureResult, null);
    }
    return futureResult;
  }

  /**
   * Closes the router and releases any resources held by the router. If the router is already closed, then this
   * method has no effect.
   * <p/>
   * After a router is closed, any further attempt to invoke Router operations will cause a {@link RouterException} with
   * error code {@link RouterErrorCode#RouterClosed} to be returned as part of the {@link Future} and {@link Callback}
   * if any.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void close()
      throws IOException {
    for (OperationController oc : ocListRef.get()) {
      oc.close();
    }
  }

  /**
   * Completes a router operation by invoking the {@code callback} and setting the {@code futureResult} with
   * {@code operationResult} (if any) and {@code exception} (if any).
   * @param futureResult the {@link FutureResult} that needs to be set.
   * @param callback that {@link Callback} that needs to be invoked. Can be null.
   * @param operationResult the result of the operation (if any).
   * @param exception {@link Exception} encountered while performing the operation (if any).
   */
  void completeOperation(FutureResult futureResult, Callback callback, Object operationResult, Exception exception) {
    try {
      futureResult.done(operationResult, exception);
      if (callback != null) {
        callback.onCompletion(operationResult, exception);
      }
    } catch (Exception e) {
      //@todo add metric.
      logger.error("Exception caught during future and callback completion", e);
    } finally {
      //@todo add metric.
    }
  }
}
