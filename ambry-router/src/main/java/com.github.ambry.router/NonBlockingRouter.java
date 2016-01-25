package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Future;


/**
 * Streaming, non-blocking router implementation for Ambry.
 */
public class NonBlockingRouter implements Router {
  private final RouterConfig routerConfig;
  private final MetricRegistry metricRegistry;
  private final ArrayList<OperationController> ocList;
  private final Random randomForOCSelection;

  public NonBlockingRouter(RouterConfig routerConfig, MetricRegistry metricRegistry)
      throws IOException {
    this.routerConfig = routerConfig;
    this.metricRegistry = metricRegistry;
    ocList = new ArrayList<OperationController>();
    for (int i = 0; i < routerConfig.routerScalingUnitCount; i++) {
      ocList.add(new OperationController());
    }
    randomForOCSelection = new Random();
  }

  private OperationController getOperationController() {
    return ocList.get((int) Utils.getRandomLong(randomForOCSelection, ocList.size()));
  }

  /**
   * Requests for the {@link BlobInfo} asynchronously and returns a future that will eventually contain the
   * {@link BlobInfo} on a successful response.
   * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
   * @return A future that would contain the {@link BlobInfo} eventually.
   */
  @Override
  public Future<BlobInfo> getBlobInfo(String blobId) {
    return getOperationController().getBlobInfo(blobId, null);
  }

  /**
   * Requests for the {@link BlobInfo} asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob for which the {@link BlobInfo} is requested.
   * @param callback The {@link Callback} which will be invoked on the completion of the request.
   * @return A future that would contain the {@link BlobInfo} eventually.
   */
  @Override
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback) {
    return getOperationController().getBlobInfo(blobId, callback);
  }

  /**
   * Requests for blob data asynchronously and returns a future that will eventually contain a
   * {@link ReadableStreamChannel} that represents blob data on a successful response.
   * @param blobId The ID of the blob for which blob data is requested.
   * @return A future that would contain a {@link ReadableStreamChannel} that represents the blob data eventually.
   */
  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId) {
    return getOperationController().getBlob(blobId, null);
  }

  /**
   * Requests for the blob data asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob for which blob data is requested.
   * @param callback The callback which will be invoked on the completion of the request.
   * @return A future that would contain a {@link ReadableStreamChannel} that represents the blob data eventually.
   */
  @Override
  public Future<ReadableStreamChannel> getBlob(String blobId, Callback<ReadableStreamChannel> callback) {
    return getOperationController().getBlob(blobId, callback);
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
    return getOperationController().putBlob(blobProperties, usermetadata, channel, null);
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
    return getOperationController().putBlob(blobProperties, usermetadata, channel, callback);
  }

  /**
   * Requests for a blob to be deleted asynchronously and returns a future that will eventually contain information
   * about whether the request succeeded or not.
   * @param blobId The ID of the blob that needs to be deleted.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId) {
    return getOperationController().deleteBlob(blobId, null);
  }

  /**
   * Requests for a blob to be deleted asynchronously and invokes the {@link Callback} when the request completes.
   * @param blobId The ID of the blob that needs to be deleted.
   * @param callback The {@link Callback} which will be invoked on the completion of a request.
   * @return A future that would contain information about whether the deletion succeeded or not, eventually.
   */
  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    return getOperationController().deleteBlob(blobId, callback);
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
    for (OperationController oc : ocList) {
      oc.close();
    }
  }
}
