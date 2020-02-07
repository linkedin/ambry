package com.github.ambry.cloud.azure;

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.replication.FindToken;
import java.util.List;


/**
 * Contains the results from the replication feed after a find next entries operation.
 */
public class FindResults {
  private final List<CloudBlobMetadata> metadataList;
  private final FindToken updatedFindToken;

  /**
   * Constructor for {@link FindResults}
   * @param metadataList {@link List} of {@link CloudBlobMetadata} objects.
   * @param updatedFindToken updated {@link FindToken}
   */
  public FindResults(List<CloudBlobMetadata> metadataList, FindToken updatedFindToken) {
    this.metadataList = metadataList;
    this.updatedFindToken = updatedFindToken;
  }

  /**
   * Return {@link List} of {@link CloudBlobMetadata} objects.
   * @return {@code metadataList}
   */
  public List<CloudBlobMetadata> getMetadataList() {
    return metadataList;
  }

  /**
   * Return updated {@link FindToken}
   * @return {@code updatedFindToken}
   */
  public FindToken getUpdatedFindToken() {
    return updatedFindToken;
  }
}
