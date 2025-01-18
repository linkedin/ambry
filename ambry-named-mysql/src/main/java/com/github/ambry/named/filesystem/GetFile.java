package com.github.ambry.named.filesystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.named.filesystem.CommonUtils.*;


public class GetFile {

  private static final Logger logger = LoggerFactory.getLogger(GetFile.class);

  public static String getBlobId(Connection connection, int accountId, int containerId, String filePath) throws SQLException {
    // Split the file path into parts
    String[] pathParts = filePath.split("/");

    UUID parentDirId = UUID.nameUUIDFromBytes("root".getBytes()); // Start with root directory ID

    // Traverse the directory path
    for (int i = 0; i < pathParts.length; i++) {
      parentDirId = getDirectoryId(connection, accountId, containerId, parentDirId, pathParts[i]);
      if (parentDirId == null) {
        logger.error("getBlobId | Directory not found {}", pathParts[i]);
        return null;
      }
    }

    // Get the blobId for the file
    String fileName = pathParts[pathParts.length - 1];
    return getFileBlobId(connection, accountId, containerId, parentDirId, fileName);
  }

  public static String getFileBlobId(Connection connection, int accountId, int containerId, UUID fileId, String fileName)
      throws SQLException {
    String sql = "SELECT f.blob_id FROM file_info f WHERE f.account_id = ? AND f.container_id = ? AND f.file_id = ?";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(fileId));

      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        return Base64.encodeBase64URLSafeString(resultSet.getBytes("blob_id"));
      }
    }
    return null; // File not found
  }
}
