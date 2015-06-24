package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.IndexValue;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


public class VerifyHardDelete {
  ClusterMap map;
  String outFile;
  String dataDir;

  public VerifyHardDelete(ClusterMap map, String dataDir, String outFile) {
    this.map = map;
    this.dataDir = dataDir;
    this.outFile = outFile;
  }

  /* public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> dataDirOpt = parser
          .accepts("dataDir", "The data directory of the partition/replica that needs to be verified for hard deletes.")
          .withRequiredArg().describedAs("data_dir").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outFileOpt =
          parser.accepts("outFile", "Output file to redirect the output ").withRequiredArg().describedAs("outFile")
              .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(dataDirOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(outFileOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      String dataDir = options.valueOf(dataDirOpt);
      String outFile = options.valueOf(outFileOpt);

      VerifyHardDelete hardDeleteVerifier = new VerifyHardDelete(map, outFile);
      hardDeleteVerifier.verifyHardDeletes(dataDir);
      hardDeleteVerifier.shutdown();
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    }
  } */

  public static void main(String args[]) {
    try {
      String basePath = "/Users/pnarayan/workspace/harddelete/run";
      String hardwareLayoutPath = basePath + "/HardwareLayoutEI.json";
      String partitionLayoutPath = basePath + "/PartitionLayoutEI.json";
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      String dataDir = basePath + "/101";
      String outFile = "/tmp/results.txt";
      VerifyHardDelete hardDeleteVerifier = new VerifyHardDelete(map, dataDir, outFile);
      hardDeleteVerifier.verifyHardDeletes();
      //hardDeleteVerifier.shutdown();
    } catch (Exception e) {
      System.out.println("Received exception: " + e);
    }
  }

  //private void shutdown() {
  //}

  private long readCleanupToken(File cleanupTokenFile)
      throws Exception {
    FindToken startToken;
    FindToken endToken;
    long parsedTokenValue = -1;
    if (cleanupTokenFile.exists()) {
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(cleanupTokenFile));
      DataInputStream stream = new DataInputStream(crcStream);
      try {
        short version = stream.readShort();
        if (version != 0) {
          throw new IllegalStateException("Unknown version encountered while parsing cleanup token");
        }
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
        FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

        startToken = factory.getFindToken(stream);
        endToken = factory.getFindToken(stream);

        ByteBuffer bytebufferToken = ByteBuffer.wrap(startToken.toBytes());
        if (bytebufferToken.getShort() != 0) {
          throw new IllegalArgumentException("token version is unknown");
        }
        int sessionIdsize = bytebufferToken.getInt();
        bytebufferToken.position(bytebufferToken.position() + sessionIdsize);
        parsedTokenValue = bytebufferToken.getLong();
        if (parsedTokenValue == -1) {
          /* Index based token, get index start offset */
          parsedTokenValue = bytebufferToken.getLong();
        }
        long crc = crcStream.getValue();
        if (crc != stream.readLong()) {
          throw new IllegalStateException("Crc mismatch while reading cleanup token");
        }
      } finally {
        stream.close();
      }
    } else {
      throw new IllegalStateException("No cleanup token");
    }
    return parsedTokenValue;
  }

  private long readAndPopulateIndex(String dataDir, long offsetInCleanupToken, HashMap<BlobId, IndexValue> blobMap)
      throws Exception {
    final String Index_File_Name_Suffix = "index";
    long lastEligibleSegmentEndOffset = -1;
    File indexDir = new File(dataDir);
    File[] indexFiles = indexDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(Index_File_Name_Suffix);
      }
    });

    Arrays.sort(indexFiles, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        if (o1 == null || o2 == null) {
          throw new NullPointerException("arguments to compare two files is null");
        }
        // File name pattern for index is offset_name. We extract the offset from
        // name to compare
        int o1Index = o1.getName().indexOf("_", 0);
        long o1Offset = Long.parseLong(o1.getName().substring(0, o1Index));
        int o2Index = o2.getName().indexOf("_", 0);
        long o2Offset = Long.parseLong(o2.getName().substring(0, o2Index));
        if (o1Offset == o2Offset) {
          return 0;
        } else if (o1Offset < o2Offset) {
          return -1;
        } else {
          return 1;
        }
      }
    });

    int numberOfKeysProcessed = 0;
    for (File indexFile : indexFiles) {
        /* Read each index file as long as it is within the endToken and populate a map with the status of the blob.*/
      DataInputStream stream = new DataInputStream(new FileInputStream(indexFile));
      //System.out.println("Dumping index " + indexFileToDump.getName() + " for " + replica);
      short version = stream.readShort();
      //System.out.println("version " + version);
      if (version == 0) {
        int keysize = stream.readInt();
        int valueSize = stream.readInt();
        long fileEndPointer = stream.readLong();
        if (fileEndPointer > offsetInCleanupToken) {
          System.out.println(
              "Reached the last segment with file end pointer " + fileEndPointer + " greater than last offset "
                  + offsetInCleanupToken);
          break;
        } else {
          lastEligibleSegmentEndOffset = fileEndPointer;
        }
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
        while (stream.available() > Crc_Size) {
          BlobId key = (BlobId) storeKeyFactory.getStoreKey(stream);
          byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
          stream.read(value);
          IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
            /*String msg =
                "key " + key + " keySize(in bytes) " + key.sizeInBytes() + " value - offset " + blobValue.getOffset()
                    + " size " + blobValue.getSize() + " Original Message Offset " + blobValue
                    .getOriginalMessageOffset() + " Flag " + blobValue.isFlagSet(IndexValue.Flags.Delete_Index)
                    + " LiveUntil " + blobValue.getTimeToLiveInMs();
            boolean isDeleted = blobValue.isFlagSet(IndexValue.Flags.Delete_Index);*/
          numberOfKeysProcessed++;
          IndexValue curValue = blobMap.get(key);
          if (curValue != null) {
            boolean isDeleted = blobValue.isFlagSet(IndexValue.Flags.Delete_Index);
            boolean isOldDeleted = curValue.isFlagSet(IndexValue.Flags.Delete_Index);
            if (isOldDeleted) {
              System.err.println("Old value was a delete and is getting replaced!");
            }
            if (!isDeleted) {
              System.err.println("New value is not a delete!");
            }
            blobMap.put(key, blobValue);
            //curValue.setFlag(IndexValue.Flags.Delete_Index);
          } else {
            blobMap.put(key, blobValue);
          }
        }
      }
    }

    System.out.println("Total number of keys processed " + numberOfKeysProcessed);
    return lastEligibleSegmentEndOffset;
  }

  private boolean verifyData(byte[] arr, boolean isHardDeleted, String type)
      throws Exception {
    boolean allzeros = true;
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] != 0) {
        allzeros = false;
        break;
      }
    }

    if (isHardDeleted && !allzeros) {
      System.err.println("Hard deleted blob's " + type + " is not all zeros");
      return false;
    }
    if (!isHardDeleted && allzeros) {
      //System.err.println("Not hard deleted blob's " + type + " is all zeros");
      return false;
    }
    return true;
  }

  /**
   *  0. Read cleanupToken:startToken offset or the last eligible segment's start offset.
   *  1. Read and store the index files in a scannable way (read all into memory) (findkey works).
   *  2. Scan the log upto the last segment's log end offset.
   *  3. For each entry:
   *   a. ensure crc passes
   *   b. If user metadata and blob are 0s, and the key is within the last eligible segment
   *      (or the cleanupToken:startToken), then findKey() should return isDeleted=true.
   *   c. If user metadata and blob are not 0s, then findKey() should return isDeleted=false.
   *   d. Any mismatch, write to a file.
   */

  public void verifyHardDeletes()
      throws Exception {
    final String Cleanup_Token_Filename = "cleanuptoken";

    try {
      FileWriter fileWriter = new FileWriter(new File(outFile));
      long offsetInCleanupToken = readCleanupToken(new File(dataDir, Cleanup_Token_Filename));
      HashMap<BlobId, IndexValue> blobMap = new HashMap<BlobId, IndexValue>();
      long lastEligibleSegmentEndOffset = readAndPopulateIndex(dataDir, offsetInCleanupToken, blobMap);

      // 2. Scan the log and check against blobMap
      final String Log_File = "log_current";
      // map of blob to offset in log.
      HashMap<BlobId, ArrayList<Long>> logBlobMap = new HashMap<BlobId, ArrayList<Long>>();

      File logFile = new File(dataDir, Log_File);
      RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
      InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());
      long currentOffset = 0;
      System.out.println("Starting scan from offset 0");
      boolean invalidEntriesInlog = false;
      long unDeletedPuts = 0;
      long hardDeletedPuts = 0;
      long deletes = 0;
      while (currentOffset < lastEligibleSegmentEndOffset) {
        long tempCurrentOffset = currentOffset;
        try {
          short version = randomAccessFile.readShort();
          if (version == 1) {
            ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
            buffer.putShort(version);
            randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
            buffer.clear();
            MessageFormatRecord.MessageHeader_Format_V1 header =
                new MessageFormatRecord.MessageHeader_Format_V1(buffer);
            // read blob id
            BlobId id;
            try {
              id = new BlobId(new DataInputStream(streamlog), map);
            } catch (IOException e) {
              invalidEntriesInlog = true;
              randomAccessFile.seek(++tempCurrentOffset);
              currentOffset = tempCurrentOffset;
              continue;
            } catch (IllegalArgumentException e) {
              invalidEntriesInlog = true;
              randomAccessFile.seek(++tempCurrentOffset);
              currentOffset = tempCurrentOffset;
              continue;
            }

            //blobId = "Id - " + id.getID();
            IndexValue indexValue = blobMap.get(id);
            boolean isHardDeleted;
            if (indexValue == null) {
              System.err.println("Key in log not found in index: " + id);
              invalidEntriesInlog = true;
              randomAccessFile.seek(++tempCurrentOffset);
              currentOffset = tempCurrentOffset;
              continue;
            } else if (indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              isHardDeleted = true;
            } else {
              isHardDeleted = false;
            }
            if (header.getBlobPropertiesRecordRelativeOffset()
                != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
              boolean print = false;
              ArrayList<Long> value = logBlobMap.get(id);
              if (value != null) {
                System.err.println("Key already present in the log at: " + value);
                System.err.println("This additional entry is in the log at: " + currentOffset);
                print = true;
                value.add(currentOffset);
              } else {
                value = new ArrayList<Long>();
                value.add(new Long(currentOffset));
                logBlobMap.put(id, value);
              }
              BlobProperties props = MessageFormatRecord.deserializeBlobProperties(streamlog);
              ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(streamlog);
              if (!verifyData(metadata.array(), isHardDeleted, "metadata")) {
                if (indexValue.getOffset() != indexValue.getOriginalMessageOffset()) {
                  System.err.println("Offset in index: " + indexValue.getOffset() + " Original offset in index: " +
                      indexValue.getOriginalMessageOffset() + " Offset in log: " + currentOffset);
                }
              }

              BlobOutput output = MessageFormatRecord.deserializeBlob(streamlog);

              if (!verifyData(Utils
                  .readBytesFromStream(output.getStream(), new byte[(int) output.getSize()], 0, (int) output.getSize()),
                  isHardDeleted, "content")) {
                if (indexValue.getOffset() != indexValue.getOriginalMessageOffset()) {
                  System.err.println("Offset: " + indexValue.getOffset() + " Original offset: " +
                      indexValue.getOriginalMessageOffset() + " Actual log offset: " + currentOffset);
                }
              }
              if (isHardDeleted) {
                fileWriter.write(id.getID());
                fileWriter.write("\n");
                hardDeletedPuts++;
              } else {
                unDeletedPuts++;
              }
            } else {
              deletes++;
              boolean deleteFlag = MessageFormatRecord.deserializeDeleteRecord(streamlog);
            }
            currentOffset += (header.getMessageSize() + buffer.capacity() + id.sizeInBytes());
          } else {
            invalidEntriesInlog = true;
            randomAccessFile.seek(++tempCurrentOffset);
            currentOffset = tempCurrentOffset;
          }
        } catch (Exception e) {
          System.out.println("Received exception" + e);
        }
      }
      String msg = ("\n============");
      msg += "\ninvalidEntriesInlog? " + (invalidEntriesInlog ? "Yes" : "No");
      msg += "\nundeleted Put Records: " + unDeletedPuts;
      msg += "\nhard deleted Put Records: " + hardDeletedPuts;
      msg += "\nDelete Records: " + deletes;
      msg += "\n============";
      fileWriter.write(msg);
      System.out.println(msg);
      fileWriter.flush();
      fileWriter.close();
    } catch (IOException ioException) {
      System.out.println("IOException thrown " + ioException);
    } catch (Exception exception) {
      System.out.println("Exception thrown " + exception);
    }
  }
}
