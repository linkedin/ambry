package com.github.ambry.cloud.azure;

import com.github.ambry.cloud.CloudBlob;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.nio.ByteBuffer;


public class AzureCloudBlob implements CloudBlob {
  private final CloudBlockBlob blob;
  private ByteBuffer prefetchedBuffer;
  private final long size;
  private boolean isPrefetched;

  public AzureCloudBlob(CloudBlockBlob blob, long blobSize) {
    this.blob = blob;
    this.size = blobSize;
    this.isPrefetched = false;
  }

  public ByteBuffer download() throws CloudStorageException {
    ByteBuffer outputBuffer = null;
    try {
      outputBuffer = ByteBuffer.allocate((int)size);
      ByteBufferOutputStream outputStream = new ByteBufferOutputStream(outputBuffer);
      blob.download(outputStream);
    } catch (StorageException ex) {
      throw new CloudStorageException("Error donwloading blob " + blob.getName(), ex);
    }
    return outputBuffer;
  }

  @Override
  public void doPrefetch() throws CloudStorageException {
    prefetchedBuffer = download();
    isPrefetched = true;
  }

  @Override
  public boolean isPrefetched() {
    return isPrefetched;
  }

  @Override
  public ByteBuffer getPrefetchedData() {
    return prefetchedBuffer;
  }
}
