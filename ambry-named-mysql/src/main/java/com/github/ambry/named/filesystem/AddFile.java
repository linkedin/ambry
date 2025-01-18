package com.github.ambry.named.filesystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.named.filesystem.CommonUtils.*;


public class AddFile {

  private static final Logger logger = LoggerFactory.getLogger(AddFile.class);

  public static void parseAndInsertPath(Connection connection, short accountId, short containerId, String filePath,
      byte[] blobId) throws Exception {
    // Remove leading slash and split the path into parts
    String[] pathParts = filePath.startsWith("/") ? filePath.substring(1).split("/") : filePath.split("/");

    UUID parentDirId = UUID.nameUUIDFromBytes("root".getBytes()); // Root directory ID
    Timestamp now = new Timestamp(System.currentTimeMillis());

    // Loop through the path to insert directories and the file
    for (int i = 0; i < pathParts.length; i++) {
      String currentPart = pathParts[i];

      if (i == pathParts.length - 1) {
        // Last part of the path is the file
        if(!directoryExists(connection, accountId, containerId, parentDirId, currentPart)){
          UUID fileId = UUID.randomUUID();
          insertDirectoryInfo(connection, accountId, containerId, parentDirId, currentPart, fileId, 0, now);
          insertFileInfo(connection, accountId, containerId, fileId, blobId);
        } else {
          logger.info("File {} already exists, account Id {}, container Id {}", filePath, accountId, containerId);
        }
      } else {
        // Intermediate parts are directories
        UUID dirId = UUID.randomUUID();
        if (!directoryExists(connection, accountId, containerId, parentDirId, currentPart)) {
          insertDirectoryInfo(connection, accountId, containerId, parentDirId, currentPart, dirId, 1, now);
        }
        parentDirId = getDirectoryId(connection, accountId, containerId, parentDirId,
            currentPart); // Update parentDirId for the next level
      }
    }
  }

  static void insertDirectoryInfo(Connection connection, short accountId, short containerId, UUID parentDirId,
      String resourceName, UUID resourceId, int resourceType, Timestamp now) throws Exception {

    String sql =
        "INSERT INTO directory_info (" + "account_id, container_id, parent_dir_id, resource_name, resource_type, "
            + "resource_id, created_ts, last_updated_ts, deleted_ts) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    String query = "";
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

      // Set parameters
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));
      preparedStatement.setString(4, resourceName);
      preparedStatement.setByte(5, (byte) resourceType);
      preparedStatement.setBytes(6, uuidToBytes(resourceId));
      preparedStatement.setTimestamp(7, now);
      preparedStatement.setTimestamp(8, now);
      preparedStatement.setTimestamp(9, null);

      query = preparedStatement.toString();
      logger.info("Inserted blob into directory table. Query {}", query);
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
  }

  static void insertFileInfo(Connection connection, short accountId, short containerId, UUID fileId, byte[] blobId)
      throws SQLException {
    String sql = "INSERT INTO file_info (" + "account_id, container_id, file_id, blob_id, deleted_ts) "
        + "VALUES (?, ?, ?, ?, ?)";
    String query = "";
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

      // Set parameters
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(fileId));
      preparedStatement.setBytes(4, blobId);
      preparedStatement.setTimestamp(5, null);

      // Execute the statement
      query = preparedStatement.toString();
      logger.info("Inserted blob into directory table. Query {}", query);
      preparedStatement.executeUpdate();
    } catch (Exception e) {
      logger.error("Failed to execute query {}, {}", query, e.getMessage());
      throw e;
    }
  }
}
