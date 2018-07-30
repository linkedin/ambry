package com.github.ambry.cloud;

import com.github.ambry.account.Account;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureCloudDestination implements CloudDestination {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);

  private String connectionString;
  private CloudStorageAccount azureAccount;
  private CloudBlobClient azureBlobClient;

  // For test mocking
  public void setAzureAccount(CloudStorageAccount azureAccount) {
    this.azureAccount = azureAccount;
  }

  public void initialize(String configSpec) throws Exception {
    // Parse the connection string and create a blob client to interact with Blob storage
    // Note: the account and blobClient will be different for every distinct Azure account.
    // Likely need thread pool to manage all the connections and reuse/garbage-collect them
    this.connectionString = configSpec;
    CloudStorageAccount azureStorageAccount =
        azureAccount != null ? azureAccount : CloudStorageAccount.parse(connectionString);
    azureBlobClient = azureStorageAccount.createCloudBlobClient();
  }

  @Override
  public boolean uploadBlob(String blobId, String containerName, long blobSize, InputStream blobInputStream) throws Exception {

    Objects.requireNonNull(blobId);
    Objects.requireNonNull(containerName);
    Objects.requireNonNull(blobInputStream);

    BlobRequestOptions options = null; // may want to set BlobEncryptionPolicy here
    OperationContext opContext = null;
    try {
      CloudBlockBlob azureBlob = getAzureBlobReference(blobId, containerName);

      if (azureBlob.exists()) {
        logger.info("Skipping upload of blob {} as it already exists in Azure container {}.", blobId, containerName);
        return false;
      }

      azureBlob.upload(blobInputStream, blobSize, null, options, opContext);
      logger.info("Uploaded blob {} to Azure container {}.", blobId, containerName);
      return true;

    } catch (Exception e) {
      // TODO: possibly transient error connecting to Azure, want to retry
      throw e;
    } finally {
      // TODO: send notification of success/fail and update metrics including upload time
    }
  }

  @Override
  public boolean deleteBlob(String blobId, String containerName) throws Exception {
    Objects.requireNonNull(blobId);
    Objects.requireNonNull(containerName);

    try {
      CloudBlockBlob azureBlob = getAzureBlobReference(blobId, containerName);

      if (!azureBlob.exists()) {
        logger.info("Skipping deletion of blob {} as it does not exist in Azure container {}.", blobId, containerName);
        return false;
      }

      azureBlob.delete();
      logger.info("Deleted blob {} from Azure container {}.", blobId, containerName);
      return true;

    } catch (Exception e) {
      // TODO: possibly fatal error with request
      throw e;
    } finally {
      // TODO: send notification of success/fail and update metrics including upload time
    }
  }

  private CloudBlockBlob getAzureBlobReference(String blobId, String containerName)
      throws URISyntaxException, InvalidKeyException, StorageException {

    if (azureBlobClient == null) {
      throw new IllegalStateException("Azure client cannot be null");
    }
    CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(containerName);

    // Create the container if it does not exist with public access.
    //System.out.println("Creating container: " + ambryContainer.getName());
    // Note: this is expensive!
    azureContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
        new OperationContext());

    //Getting a blob reference
    CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId);
    return azureBlob;
  }
}
