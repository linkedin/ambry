package com.github.ambry.named.filesystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static com.github.ambry.named.filesystem.CommonUtils.*;


public class DeleteDirectoryOrFiles {

  static void markDirectorySoftDeleted(Connection connection, int accountId, int containerId, UUID resourceId) throws SQLException {
    String sql = "UPDATE directory_info "
        + "SET deleted_ts = ?, gg_status = 'd' "
        + "WHERE account_id = ? AND container_id = ? AND resource_id = ?";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setTimestamp(1, Timestamp.from(Instant.now()));
      preparedStatement.setInt(2, accountId);
      preparedStatement.setInt(3, containerId);
      preparedStatement.setBytes(4, uuidToBytes(resourceId));

      int rowsUpdated = preparedStatement.executeUpdate();
      if (rowsUpdated > 0) {
        System.out.println("Marked directory as soft deleted: " + resourceId);
      } else {
        throw new SQLException("Failed to mark directory as deleted: " + resourceId);
      }
    }
  }

  static void addToPendingDeletes(Connection connection, int accountId, int containerId, UUID resourceId) throws SQLException {
    String sql = "INSERT INTO pending_deletes (account_id, container_id, pending_dir_id, insert_ts, deleted_ts) "
        + "VALUES (?, ?, ?, ?, ?)";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      Timestamp now = Timestamp.from(Instant.now());
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(resourceId));
      preparedStatement.setTimestamp(4, now); // Insert timestamp
      preparedStatement.setTimestamp(5, now); // Deleted timestamp

      preparedStatement.executeUpdate();
      System.out.println("Added directory to pending deletes: " + resourceId);
    }
  }


  public static void deleteFolderRecursively(Connection connection, int accountId, int containerId, String folderPath) throws SQLException {
    // Get the parent directory ID for the folder
    UUID parentDirId = getLeafDirectoryId(connection, accountId, containerId, folderPath);

    if (parentDirId == null) {
      throw new SQLException("Folder not found: " + folderPath);
    }

    // Recursively delete contents of the folder
    deleteContentsUnderDirectory(connection, accountId, containerId, parentDirId);

    // Delete the folder itself
    deleteDirectory(connection, accountId, containerId, parentDirId, folderPath);
  }

  public static void deleteContentsUnderDirectory(Connection connection, int accountId, int containerId, UUID parentDirId) throws SQLException {
    // Delete files in the current directory
    String sqlDeleteFiles = "DELETE FROM file_info WHERE file_id IN ("
        + "SELECT resource_id FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_type = 1)";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDeleteFiles)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));
      int filesDeleted = preparedStatement.executeUpdate();
      System.out.println("Deleted " + filesDeleted + " file(s) under directory: " + parentDirId);
    }

    // Get subdirectories to delete them recursively
    String sqlGetSubdirectories = "SELECT resource_id, resource_name FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_type = 1";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlGetSubdirectories)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        UUID subDirId = bytesToUuid(resultSet.getBytes("resource_id"));
        String subDirName = resultSet.getString("resource_name");

        // Recursively delete subdirectory contents
        deleteContentsUnderDirectory(connection, accountId, containerId, subDirId);

        // Delete the subdirectory itself
        deleteDirectory(connection, accountId, containerId, subDirId, subDirName);
      }
    }
  }

  public static void deleteDirectory(Connection connection, int accountId, int containerId, UUID resourceId, String resourceName) throws
                                                                                                                            SQLException {
    String sqlDeleteDirectory = "DELETE FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND resource_id = ? AND resource_name = ?";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDeleteDirectory)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(resourceId));
      preparedStatement.setString(4, resourceName);

      int rowsDeleted = preparedStatement.executeUpdate();
      if (rowsDeleted > 0) {
        System.out.println("Deleted directory: " + resourceName);
      }
    }
  }

}
