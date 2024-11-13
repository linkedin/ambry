package com.github.ambry.store;

import com.github.ambry.clustermap.DiskId;
import java.io.File;
import java.util.HashSet;
import java.util.Set;


public class PartitionFinder {
  /**
   * Find all the partition directories on the disk.
   * @param disk an instance of DiskId to search for partition directories.
   * @return A {@code Set} of partition directories on the disk. The set will be empty if no partition
   *        directories are found, or if something goes wrong (e.g. mount path does not exist).
   */
  public Set<String> findPartitionsOnDisk(DiskId disk) {
    Set<String> partitionDirs = new HashSet<>();
    File mount = new File(disk.getMountPath());

    if(mount.exists() && mount.isDirectory())
    {
      File[] ambryDirs = mount.listFiles(File::isDirectory);
      if(ambryDirs != null)
      {
        for(File dir : ambryDirs)
        {
          // Tommy: If we store just the leaf name from File.getName() will that work
          //        when comparing with the directory names we get from ReplicaId??
          //        AmbryPartition::toPathString() returns the partition ID as a string.
          String dirName = dir.getName();
          if(looksLikePartition(dirName))
          {
            partitionDirs.add(dirName);
          }
        }
      }
    }
    return partitionDirs;
  }

  /**
   * Checks whether a directory name looks like a partition.
   * @param directoryName the name of the directory to check.
   * @return True if the directory name looks like a partition, false otherwise.
   */
  private boolean looksLikePartition(String directoryName) {
    try {
      // The convention is simply to use Java long as partition IDs.
      Long.parseLong(directoryName);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }
}
