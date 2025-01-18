package com.github.ambry.named.filesystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.github.ambry.named.filesystem.CommonUtils.*;


public class ListDirectoriesAndFiles {

  public static List<String> listContentsUnderFolder(Connection connection, int accountId, int containerId, String folderPath)
      throws SQLException {

    List<String> contents = new ArrayList<>();

    // Get the parent directory ID for the folder
    UUID parentDirId = getLeafDirectoryId(connection, accountId, containerId, folderPath);

    if (parentDirId == null) {
      throw new SQLException("Folder not found: " + folderPath);
    }

    // Query to find directories under the given parent directory
    String sqlDirectories = "SELECT resource_name FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_type = 1  AND deleted_ts IS NULL";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDirectories)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        contents.add("[Directory] " + resultSet.getString("resource_name"));
      }
    }

    // Query to find files under the given parent directory

    String sqlFiles = "SELECT resource_name FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_type = 0 AND deleted_ts IS NULL";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlFiles)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        contents.add("[File] " + resultSet.getString("resource_name"));
      }
    }

    return contents;
  }

  public static List<String> listFilesRecursively(Connection connection, int accountId, int containerId, String folderPath)
      throws SQLException {
    List<String> files = new ArrayList<>();

    // Get the parent directory ID for the folder
    UUID parentDirId = getLeafDirectoryId(connection, accountId, containerId, folderPath);

    if (parentDirId == null) {
      return null;
    }

    // Recursively list files
    listFilesUnderDirectory(connection, accountId, containerId, parentDirId, folderPath, files);

    return files;
  }

  public static void listFilesUnderDirectory(Connection connection, int accountId, int containerId, UUID parentDirId,
      String currentPath, List<String> files) throws SQLException {

    // Query to find files in the current directory
    String sqlFiles = "SELECT resource_name FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_type = 0 AND deleted_ts IS NULL";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlFiles)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        String fileName = resultSet.getString("resource_name");
        files.add(currentPath + "/" + fileName);
      }
    }

    // Query to find subdirectories in the current directory
    String sqlDirectories = "SELECT resource_name, resource_id, deleted_ts FROM directory_info "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_type = 1";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlDirectories)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));

      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        String subdirectoryName = resultSet.getString("resource_name");
        UUID subdirectoryId = bytesToUuid(resultSet.getBytes("resource_id"));
        Timestamp deletedTs = resultSet.getTimestamp("deleted_ts");
        if(deletedTs != null){
         continue;
        }
        // Recursively list files in the subdirectory
        listFilesUnderDirectory(connection, accountId, containerId, subdirectoryId,
            currentPath + "/" + subdirectoryName, files);
      }
    }
  }
}
