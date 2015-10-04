package com.github.ambry.rest;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.util.concurrent.Future;


/**
 * BlobStorageService defines a service that forms the bridge b/w a RESTful frontend and a storage backend (or something
 * that communicates with a storage backend).
 * <p/>
 * Typically, a BlobStorageService is expected to receive requests from the RESTful frontend, handle them as required
 * and either make a callback with the handling results (if immediately available) or pass control to another
 * component that do further handling and make callbacks. The information received from the frontend should be enough
 * to perform these functions.
 * <p/>
 * Most operations are performed async and therefore a {@link Future} that will eventually contain the result of the
 * operation is returned. On operation completion, the callback provided (if not null) is invoked.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface BlobStorageService {

  /**
   * Does startup tasks for the BlobStorageService. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if BlobStorageService is unable to start.
   */
  public void start()
      throws InstantiationException;

  /**
   * Does shutdown tasks for the BlobStorageService. When the function returns, shutdown is FULLY complete.
   */
  public void shutdown();

  /**
   * Handles a GET operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request.
   * <p/>
   * It is expected that the returned {@link Future} will eventually contain the result of the operation and the
   * {@code callback} provided (if not null) will be invoked when the operation is complete.
   * <p/>
   * Any exception is communicated through the {@link Future} and {@code callback} (if not not null).
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param callback the {@link Callback} that needs to be invoked on operation completion (can be null).
   * @return a {@link Future} that will eventually contain the result of the operation.
   */
  public Future<ReadableStreamChannel> handleGet(RestRequest restRequest, Callback<ReadableStreamChannel> callback);

  /**
   * Handles a POST operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request.
   * <p/>
   * It is expected that the returned {@link Future} will eventually contain the result of the operation and the
   * {@code callback} provided (if not null) will be invoked when the operation is complete.
   * <p/>
   * Any exception is communicated through the {@link Future} and {@code callback} (if not not null).
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param callback the {@link Callback} that needs to be invoked on operation completion (can be null).
   * @return a {@link Future} that will eventually contain the result of the operation.
   */
  public Future<String> handlePost(RestRequest restRequest, Callback<String> callback);

  /**
   * Handles a DELETE operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request.
   * <p/>
   * It is expected that the returned {@link Future} will eventually contain the result of the operation and the
   * {@code callback} provided (if not null) will be invoked when the operation is complete.
   * <p/>
   * Any exception is communicated through the {@link Future} and {@code callback} (if not not null).
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param callback the {@link Callback} that needs to be invoked on operation completion (can be null).
   * @return a {@link Future} that will eventually contain the result of the operation.
   */
  public Future<Void> handleDelete(RestRequest restRequest, Callback<Void> callback);

  /**
   * Handles a HEAD operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request.
   * <p/>
   * It is expected that the returned {@link Future} will eventually contain the result of the operation and the
   * {@code callback} provided (if not null) will be invoked when the operation is complete.
   * <p/>
   * Any exception is communicated through the {@link Future} and {@code callback} (if not not null).
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param callback the {@link Callback} that needs to be invoked on operation completion (can be null).
   * @return a {@link Future} that will eventually contain the result of the operation.
   */
  public Future<BlobInfo> handleHead(RestRequest restRequest, Callback<BlobInfo> callback);
}
