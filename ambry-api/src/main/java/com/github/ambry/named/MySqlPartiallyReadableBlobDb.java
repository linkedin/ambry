package com.github.ambry.named;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.MySqlPartiallyReadableBlobDbConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlPartiallyReadableBlobDb implements PartiallyReadableBlobDb {
  private static final Logger logger = LoggerFactory.getLogger(MySqlPartiallyReadableBlobDb.class);

  // table names
  private static final String BLOB_CHUNKS = "blob_chunks";
  private static final String BLOB_INFO = "blob_info";
  // column names
  private static final String ACCOUNT_NAME = "account_name";
  private static final String CONTAINER_NAME = "container_name";
  private static final String BLOB_NAME = "blob_name";
  private static final String CHUNK_OFFSET = "chunk_offset";
  private static final String CHUNK_SIZE = "chunk_size";
  private static final String CHUNK_ID = "chunk_id";
  private static final String STATUS = "status";
  private static final String LAST_UPDATED_TS = "last_updated_ts";

  private static final String BLOB_SIZE = "blob_size";
  private static final String SERVICE_ID = "service_id";
  private static final String USER_METADATA = "user_metadata";

  private static final String PK_MATCH = String.format("(%s, %s, %s) = (?, ?, ?)", ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME);
  private static final String ORDER_MATCH = String.format("(%s) >= (?)", CHUNK_OFFSET);
  private static final String FIND_MAX_OFFSET_RECORD = String.format("SELECT MAX(%s) FROM (SELECT * FROM %s) AS BLOB_CHUNKS WHERE %s",
      CHUNK_OFFSET, BLOB_CHUNKS, PK_MATCH);

  /**
   * Insert a new chunk record into the database.
   */
  private static final String INSERT_CHUNK_QUERY =
      String.format("INSERT INTO %s (%s, %s, %s, %5$s, %6$s, %7$s, %8$s, %9$s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          BLOB_CHUNKS, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, CHUNK_OFFSET, CHUNK_SIZE, CHUNK_ID, STATUS, LAST_UPDATED_TS);

  /**
   * Get a list of chunks records that match the priority keys and has offset greater than or equal the parameter set.
   */
  private static final String GET_CHUNKS_QUERY =
      String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s AND %s ORDER BY %s", CHUNK_OFFSET, CHUNK_SIZE, CHUNK_ID, STATUS,
          LAST_UPDATED_TS, BLOB_CHUNKS, PK_MATCH, ORDER_MATCH, CHUNK_OFFSET);

  /**
   * Update the status of chunk with the highest offset in the composite blob.
   */
  private static final String UPDATE_STATUS_QUERY = String.format("UPDATE %s SET %s = ? WHERE %s AND %s = (%s)",
      BLOB_CHUNKS, STATUS, PK_MATCH, CHUNK_OFFSET, FIND_MAX_OFFSET_RECORD);

  /**
   * Get the blob info from database that matches the priority keys.
   */
  private static final String GET_BLOB_INFO_QUERY = String.format("SELECT %s, %s, %s FROM %s WHERE %s", BLOB_SIZE,
      SERVICE_ID, USER_METADATA, BLOB_INFO, PK_MATCH);

  /**
   * Insert a new blob info record into the database.
   */
  private static final String INSERT_BLOB_INFO_QUERY = String.format("INSERT INTO %s (%s, %s, %s, %5$s, %6$s, %7$s) VALUES (?, ?, ?, ?, ?, ?)",
      BLOB_INFO, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME, BLOB_SIZE, SERVICE_ID, USER_METADATA);

  private final String URL;
  private final String USERNAME;
  private final String PASSWORD;

  public MySqlPartiallyReadableBlobDb(MySqlPartiallyReadableBlobDbConfig config) {
    this.URL = config.url;
    this.USERNAME = config.username;
    this.PASSWORD = config.password;
  }

  @Override
  public List<PartiallyReadableBlobRecord> get(String accountName, String containerName, String blobName,
      long startingChunkOffset) throws RestServiceException {
    List<PartiallyReadableBlobRecord> chunksRecord = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement statement = connection.prepareStatement(GET_CHUNKS_QUERY)) {

      statement.setString(1, accountName);
      statement.setString(2, containerName);
      statement.setString(3, blobName);
      statement.setLong(4, startingChunkOffset);

      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          long chunkOffset = resultSet.getLong(1);
          long chunkSize = resultSet.getLong(2);
          String chunkId = resultSet.getString(3);
          String status = resultSet.getString(4);
          long lastUpdatedTs = resultSet.getLong(5);

          if (status.equals(PartialPutStatus.ERROR.name())) {
            throw buildException("Error occurred during the POST process", RestServiceErrorCode.InternalServerError,
                accountName, containerName, blobName);
          }
          chunksRecord.add(new PartiallyReadableBlobRecord(accountName, containerName, blobName, chunkId, chunkOffset,
              chunkSize, lastUpdatedTs, status));
        }
      }
    }
    catch (SQLException e) {
      throw buildException("MySQL connection error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
          accountName, containerName, blobName);
    }
    return chunksRecord;
  }

  @Override
  public void put(PartiallyReadableBlobRecord record) throws RestServiceException {
    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement statement = connection.prepareStatement(INSERT_CHUNK_QUERY)) {

      statement.setString(1, record.getAccountName());
      statement.setString(2, record.getContainerName());
      statement.setString(3, record.getBlobName());
      statement.setLong(4, record.getChunkOffset());
      statement.setLong(5, record.getChunkSize());
      statement.setString(6, record.getChunkId());
      statement.setString(7, record.getStatus());
      statement.setLong(8, record.getLastUpdatedTs());

      try {
        int rowUpdated = statement.executeUpdate();
        if (rowUpdated == 0) {
          // error for put fails as there might be a conflict
          throw buildException("Put unsuccessfully", RestServiceErrorCode.Conflict, record.getAccountName(),
              record.getContainerName(), record.getBlobName());
        } else {
          logger.trace("Successfully inserted record with account='" + record.getAccountName() +
              "', container='" + record.getContainerName() + "', name='" + record.getBlobName() + "'"
              + " and chunkId='" + record.getChunkId() + "'");
        }
      }
      catch (SQLException e) {
        throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
            record.getAccountName(), record.getContainerName(), record.getBlobName());
      }
    }
    catch (SQLException e) {
      throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
          record.getAccountName(), record.getContainerName(), record.getBlobName());
    }
  }

  @Override
  public void updateStatus(String accountName, String containerName, String blobName) throws RestServiceException{
    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement statement = connection.prepareStatement(UPDATE_STATUS_QUERY)) {

      statement.setString(1, PartialPutStatus.SUCCESS.name());
      statement.setString(2, accountName);
      statement.setString(3, containerName);
      statement.setString(4, blobName);
      statement.setString(5, accountName);
      statement.setString(6, containerName);
      statement.setString(7, blobName);

      try {
        int rowUpdated = statement.executeUpdate();

        if (rowUpdated == 0) {
          throw buildException("Updated unsuccessfully", RestServiceErrorCode.Conflict, accountName, containerName,
              blobName);
        } else {
          logger.trace("Successfully update record with account='" + accountName + "', container='" + containerName + "', name='"
              + blobName + "'");
        }
      }
      catch (SQLException e) {
        throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
            accountName, containerName, blobName);
      }
    }
    catch (SQLException e) {
      throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
          accountName, containerName, blobName);
    }
  }

  @Override
  public BlobInfo getBlobInfo(String accountName, String containerName, String blobName, RestRequest restRequest)
      throws RestServiceException {
    Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
    Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());

    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement statement = connection.prepareStatement(GET_BLOB_INFO_QUERY)) {

      statement.setString(1, accountName);
      statement.setString(2, containerName);
      statement.setString(3, blobName);

      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          // the blob is still being uploaded thus the blobSize and userMetadata should not be completed and matter at
          // this time
          return new BlobInfo(new BlobProperties(1000000, "CUrlUpload", account.getId(), container.getId(),
              container.isEncrypted()), new byte[0]);
        }
        long blobSize = resultSet.getLong(1);
        String serviceId = resultSet.getString(2);
        byte[] userMetadata = resultSet.getBytes(3);
        BlobProperties blobProperties = new BlobProperties(blobSize, serviceId, account.getId(), container.getId(),
            container.isEncrypted());
        return new BlobInfo(blobProperties, userMetadata);
      }
    }
    catch (SQLException e) {
      throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError, accountName,
          containerName, blobName);
    }
  }

  @Override
  public void putBlobInfo(String accountName, String containerName, String blobName, long blobSize, String serviceId,
      byte[] userMetadata) throws RestServiceException {
    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement statement = connection.prepareStatement(INSERT_BLOB_INFO_QUERY)) {

      statement.setString(1, accountName);
      statement.setString(2, containerName);
      statement.setString(3, blobName);
      statement.setLong(4, blobSize);
      statement.setString(5, serviceId);
      statement.setBytes(6, userMetadata);

      try {
        int rowUpdated = statement.executeUpdate();

        if (rowUpdated == 0) {
          // error for put fails as there might be a conflict
          throw buildException("Put unsuccessfully", RestServiceErrorCode.Conflict, accountName, containerName, blobName);
        } else {
          logger.trace("Successfully inserted blob info with account='" + accountName +
              "', container='" + containerName + "', name='" + blobName + "'");
        }
      }
      catch (SQLException e) {
        throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
            accountName, containerName, blobName);
      }
    }
    catch (SQLException e) {
      throw buildException("MySQL error: " + e.getMessage(), RestServiceErrorCode.InternalServerError,
          accountName, containerName, blobName);
    }
  }

  private static RestServiceException buildException(String message, RestServiceErrorCode errorCode, String accountName,
      String containerName, String blobName) {
    return new RestServiceException(
        message + "; account='" + accountName + "', container='" + containerName + "', name='" + blobName + "'",
        errorCode);
  }
}
