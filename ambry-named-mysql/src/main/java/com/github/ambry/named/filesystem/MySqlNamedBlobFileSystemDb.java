package com.github.ambry.named.filesystem;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.named.DeleteResult;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.named.PutResult;
import com.github.ambry.named.StaleNamedBlob;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.named.filesystem.CommonUtils.*;
import static com.github.ambry.named.filesystem.DeleteDirectoryOrFiles.*;
import static com.github.ambry.named.filesystem.GetFile.*;
import static com.github.ambry.named.filesystem.ListDirectoriesAndFiles.*;
import static com.github.ambry.named.filesystem.AddFile.*;


public class MySqlNamedBlobFileSystemDb implements NamedBlobDb {

  private static final Logger logger = LoggerFactory.getLogger(MySqlNamedBlobFileSystemDb.class);
  private final AccountService accountService;
  private final MySqlNamedBlobDbConfig config;
  private final DataSource dataSource;

  public MySqlNamedBlobFileSystemDb(AccountService accountService, MySqlNamedBlobDbConfig config,
      DataSourceFactory dataSourceFactory, String localDatacenter, MetricRegistry metricRegistry, Time time) {
    this.accountService = accountService;
    this.config = config;
    List<MySqlUtils.DbEndpoint> dbEndpoints = MySqlUtils.getDbEndpointsPerDC(config.dbInfo).get(localDatacenter);
    MySqlUtils.DbEndpoint dbEndpoint =
        dbEndpoints.stream().filter(MySqlUtils.DbEndpoint::isWriteable).collect(Collectors.toList()).get(0);
    logger.info("FileSystem | DB Endpoint {}", dbEndpoint.toJson());
    dataSource = dataSourceFactory.getDataSource(dbEndpoint);
  }

  @Override
  public CompletableFuture<PutResult> put(NamedBlobRecord record, NamedBlobState state, Boolean isUpsert) {

    CompletableFuture<PutResult> completableFuture = new CompletableFuture<>();
    // Look up account and container IDs. This is common logic needed for all types of transactions.
    Account account = accountService.getAccountByName(record.getAccountName());
    Container container = account.getContainerByName(record.getContainerName());
    short accountId = account.getId();
    short containerId = container.getId();
    try {
      try (Connection connection = dataSource.getConnection()){
        // Parse the file path and insert directories and file
        parseAndInsertPath(connection, accountId, containerId, record.getBlobName(),
            Base64.decodeBase64(record.getBlobId()));
        NamedBlobRecord namedBlobRecord =
            new NamedBlobRecord(record.getAccountName(), record.getContainerName(), record.getBlobName(),
                record.getBlobId(), record.getExpirationTimeMs());
        PutResult result = new PutResult(namedBlobRecord);
        completableFuture.complete(result);
        logger.info("MySqlNamedBlobFileSystemDb put {} ", record);
      }
    } catch (Exception e) {
      logger.error("MySqlNamedBlobFileSystemDb put() error {} ", record, e);
      completableFuture.completeExceptionally(e);
    }
    return completableFuture;
  }

  public CompletableFuture<Void> renameDirectory(NamedBlobRecord record, String newName) {

    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    // Look up account and container IDs. This is common logic needed for all types of transactions.
    Account account = accountService.getAccountByName(record.getAccountName());
    Container container = account.getContainerByName(record.getContainerName());
    short accountId = account.getId();
    short containerId = container.getId();
    try {
      try (Connection connection = dataSource.getConnection()){
        boolean result = CommonUtils.renameDirectory(connection, accountId, containerId, record.getBlobName(), newName);
        if(!result){
          completableFuture.completeExceptionally(
              buildException("FileSystem | Rename, Directory not found", RestServiceErrorCode.NotFound, record.getAccountName(),
                  record.getContainerName(), record.getBlobName()));
        } else {
          completableFuture.complete(null);
        }
        logger.info("MySqlNamedBlobFileSystemDb rename successful {} ", record);
      }
    } catch (Exception e) {
      logger.error("MySqlNamedBlobFileSystemDb rename() error {} ", record, e);
      completableFuture.completeExceptionally(e);
    }
    return completableFuture;
  }

  @Override
  public CompletableFuture<NamedBlobRecord> get(String accountName, String containerName, String blobName,
      GetOption option) {
    // Look up account and container IDs. This is common logic needed for all types of transactions.
    CompletableFuture<NamedBlobRecord> future = new CompletableFuture<>();
    Account account = accountService.getAccountByName(accountName);
    Container container = account.getContainerByName(containerName);
    short accountId = account.getId();
    short containerId = container.getId();
    try {
      try (Connection connection = dataSource.getConnection()){
        String blobId = getBlobId(connection, accountId, containerId, blobName);
        if (blobId != null) {
          logger.info("FileSystem | MySqlNamedBlobFileSystemDb get(), Blob ID for " + blobName + ": " + blobId);
          NamedBlobRecord namedBlobRecord =
              new NamedBlobRecord(accountName, containerName, blobName, blobId, Utils.Infinite_Time);
          future.complete(namedBlobRecord);
        } else {
          logger.error("FileSystem | MySqlNamedBlobFileSystemDb get(), File not found: " + blobName);
          future.completeExceptionally(
              buildException("Blob not found", RestServiceErrorCode.NotFound, accountName,
                  containerName, blobName));
        }
      }
    } catch (Exception e) {
      logger.error(
          "FileSystem | MySqlNamedBlobFileSystemDb get(), account name {}, container name {}, blobName {}, error {} ",
          accountName, containerName, blobName, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public CompletableFuture<Page<NamedBlobRecord>> list(String accountName, String containerName, String blobNamePrefix,
      String pageToken, Integer maxKey) {

    // Look up account and container IDs. This is common logic needed for all types of transactions.
    CompletableFuture<Page<NamedBlobRecord>> completableFuture = new CompletableFuture<>();
    Account account = accountService.getAccountByName(accountName);
    Container container = account.getContainerByName(containerName);
    short accountId = account.getId();
    short containerId = container.getId();
    try {
      try (Connection connection = dataSource.getConnection()){
        List<String> files = listFilesRecursively(connection, accountId, containerId, blobNamePrefix);
        if(files == null){
          completableFuture.completeExceptionally(
              buildException("FileSystem | Path not found", RestServiceErrorCode.NotFound, accountName,
                  containerName, blobNamePrefix));
          return completableFuture;
        }
        List<NamedBlobRecord> entries = new ArrayList<>();
        for(String file : files){
          entries.add(new NamedBlobRecord(accountName, containerName, file, null, -1));
        }
        logger.info(
            "FileSystem | MySqlNamedBlobFileSystemDb list recursively under account {}, container {}, prefix {}, contents {}",
            accountName, containerName, blobNamePrefix, files);
        completableFuture.complete(new Page<>(entries, null));
      }
    } catch (Exception e) {
      logger.error(
          "FileSystem | MySqlNamedBlobFileSystemDb list recursively(), account name {}, container name {}, blobName {}, error {} ",
          accountName, containerName, blobNamePrefix, e);
      completableFuture.completeExceptionally(e);
    }
    return completableFuture;
  }

  @Override
  public CompletableFuture<DeleteResult> delete(String accountName, String containerName, String blobName) {
    // Look up account and container IDs. This is common logic needed for all types of transactions.
    CompletableFuture<DeleteResult> completableFuture = new CompletableFuture<>();
    Account account = accountService.getAccountByName(accountName);
    Container container = account.getContainerByName(containerName);
    short accountId = account.getId();
    short containerId = container.getId();
    try {
      try (Connection connection = dataSource.getConnection()){
        // Get the directory ID for the folder
        UUID parentDirId = getLeafDirectoryId(connection, accountId, containerId, blobName);

        if (parentDirId == null) {
          completableFuture.completeExceptionally(
              buildException("FileSystem | DELETE: Directory/Blob not found", RestServiceErrorCode.NotFound, accountName,
                  containerName, blobName));
          return completableFuture;
        }

        // Mark the directory as soft deleted
        markDirectorySoftDeleted(connection, accountId, containerId, parentDirId);

        // Add the entry to the pending deletes table
        addToPendingDeletes(connection, accountId, containerId, parentDirId);

        completableFuture.complete(new DeleteResult("", false));
      }
    } catch (Exception e) {
      logger.error(
          "FileSystem | MySqlNamedBlobFileSystemDb delete(), account name {}, container name {}, blobName {}, error {} ",
          accountName, containerName, blobName, e);
      completableFuture.completeExceptionally(e);
    }

    return completableFuture;
  }

  public CompletableFuture<Page<NamedBlobRecord>> listDirectory(String accountName, String containerName,
      String folderPath) {
    // Look up account and container IDs. This is common logic needed for all types of transactions.
    CompletableFuture<Page<NamedBlobRecord>> completableFuture = new CompletableFuture<>();
    List<NamedBlobRecord> entries = new ArrayList<>();
    Account account = accountService.getAccountByName(accountName);
    Container container = account.getContainerByName(containerName);
    short accountId = account.getId();
    short containerId = container.getId();
    try {
      try (Connection connection = dataSource.getConnection()){
        List<String> contents = listContentsUnderFolder(connection, accountId, containerId, folderPath);
        for (String content : contents) {
          entries.add(new NamedBlobRecord(accountName, containerName, content, null, -1));
        }
        logger.info(
            "FileSystem | MySqlNamedBlobFileSystemDb list non recursively under account {}, container {}, prefix {}, contents {}",
            accountName, containerName, folderPath, contents);
        completableFuture.complete(new Page<>(entries, null));
      }
    } catch (Exception e) {
      logger.error(
          "FileSystem | MySqlNamedBlobFileSystemDb list non recursively(), account name {}, container name {}, blobName {}, error {} ",
          accountName, containerName, folderPath, e);
      completableFuture.completeExceptionally(e);
    }

    return completableFuture;
  }


  @Override
  public CompletableFuture<PutResult> updateBlobTtlAndStateToReady(NamedBlobRecord record) {
    return null;
  }

  @Override
  public CompletableFuture<List<StaleNamedBlob>> pullStaleBlobs() {
    return null;
  }

  @Override
  public CompletableFuture<Integer> cleanupStaleData(List<StaleNamedBlob> staleRecords) {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * A factory that produces a configured {@link DataSource} based on supplied configs.
   */
  public interface DataSourceFactory {
    /**
     * @param dbEndpoint {@link MySqlUtils.DbEndpoint} object containing the database connection settings to use.
     * @return an instance of {@link DataSource} for the provided {@link MySqlUtils.DbEndpoint}.
     */
    DataSource getDataSource(MySqlUtils.DbEndpoint dbEndpoint);
  }

  private static RestServiceException buildException(String message, RestServiceErrorCode errorCode, String accountName,
      String containerName, String blobName) {
    return new RestServiceException(
        message + "; account='" + accountName + "', container='" + containerName + "', name='" + blobName + "'",
        errorCode);
  }
}
