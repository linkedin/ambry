package com.github.ambry.router;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import java.io.Closeable;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.Future;


/**
 * The router interface for Ambry that helps to interact with Ambry server.
 */
public interface Router extends Closeable {
  /**
   * Request for the BlobInfo asynchronously and returns a future which will finally contain the BlobInfo on a
   * successful response.
   * @param blobId The ID of the blob for which the BlobInfo is requested
   * @return A future that would contain the BlobInfo eventually
   */
  public Future<BlobInfo> getBlobInfo(String blobId);
  /**
   * Request for the BlobInfo asynchronously and invokes the callback when the request completes.
   * @param blobId The ID of the blob for which the BlobInfo is requested
   * @param callback The callback which will be invoked on the completion of a request
   * @return A future that would contain the BlobInfo eventually
   */
  public Future<BlobInfo> getBlobInfo(String blobId, Callback<BlobInfo> callback);
  /**
   * Request for the blob content asynchronously and returns a future which will finally contain
   * the Blob on a successful response.
   * @param blobId The ID of the blob for which the Blob is requested
   * @return A future that would contain the Blob eventually
   */
  public Future<BlobOutput> getBlob(String blobId);
  /**
   * Request for the blob content asynchronously and invokes the callback when the request completes.
   * @param blobId The ID of the blob for which the Blob is requested
   * @param callback The callback which will be invoked on the completion of a request
   * @return A future that would contain the blob output eventually
   */
  public Future<BlobOutput> getBlob(String blobId, Callback<BlobOutput> callback);
  /**
   * Request for a new blob to be put asynchronously and returns a future which will finally contain
   * the BlobId of the new blob on a successful response.
   * @param blobProperties The blob properties of the blob
   * @param usermetadata An optional usermetadata about the blob. This can be null.
   * @param channel The Readable channel that contains the content of the blob.
   * @return A future that contains the blob Id eventually
   */
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableByteChannel channel);
  /**
   * Request for a new blob to be put asynchronously and invokes the callback when the request completes.
   * @param blobProperties The blob properties of the blob
   * @param usermetadata An optional usermetadata about the blob. This can be null.
   * @param channel The Readable channel that contains the content of the blob.
   * @param callback The callback which will be invoked on the completion of a request
   * @return A future that contains the blob Id eventually
   */
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableByteChannel channel, Callback<String> callback);
  /**
   * Request for a blob to be deleted asynchronously and returns a future which will finally contain if the request
   * succeeded or not.
   * @param blobId The Id of the blob that needs to be deleted
   * @return A future that contains response about whether the deletion succeeded or not eventually
   */
  public Future<Void> deleteBlob(String blobId);
  /**
   * Request for a blob to be deleted asynchronously and invokes the callback when the request completes.
   * @param blobId The Id of the blob that needs to be deleted
   * @param callback The callback which will be invoked on the completion of a request
   * @return A future that contains response about whether the deletion succeeded or not eventually
   */
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback);
  /**
   * Close the router
   */
  public void close();
}
