package com.github.ambry.tools.admin;

import com.github.ambry.commons.BlobId;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.IndexValue;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.Utils;
import java.io.EOFException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.io.File;


/**
 * Dumps the log or the index given a file path
 */
public class DumpData {

  String outFile;
  FileWriter fileWriter;
  ClusterMap map;

  public DumpData(ClusterMap map) {
    this.map = map;
  }

  public DumpData(String outFile, ClusterMap map)
      throws IOException {
    this(map);
    init(outFile);
  }

  public DumpData(String outFile, FileWriter fileWriter, ClusterMap map)
      throws IOException {
    this(map);
    this.outFile = outFile;
    this.fileWriter = fileWriter;
  }

  public void init(String outFile) {
    try {
      if (outFile != null) {
        this.outFile = outFile;
        fileWriter = new FileWriter(new File(outFile));
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to create File " + this.outFile);
    }
  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> fileToReadOpt = parser.accepts("fileToRead",
          "The file that needs to be dumped. Index file incase of \"DumpIndex\", "
              + "log file incase of \"DumpLog\", replicatoken file incase of \"DumpReplicatoken\" and index file incase "
              + "of \"CompareIndexToLog\" ").withRequiredArg().describedAs("file_to_read").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfOperationOpt = parser.accepts("typeOfOperation",
          "The type of operation to be performed - DumpLog or DumpIndex or DumpReplicatoken or CompareIndexToLog")
          .withRequiredArg().describedAs("The type of Operation to be performed").ofType(String.class)
          .defaultsTo("log");

      ArgumentAcceptingOptionSpec<String> listOfBlobs =
          parser.accepts("listOfBlobs", "List Of Blobs to look for during log/index dump").withRequiredArg()
              .describedAs("List of blobs, comma separated").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> startOffsetOpt =
          parser.accepts("startOffset", "Log Offset to start dumping from").withRequiredArg().describedAs("startOffset")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> endOffsetOpt =
          parser.accepts("endOffset", "Log Offset to end dumping").withRequiredArg().describedAs("endOffset")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> logFileToCompareOpt =
          parser.accepts("logFileToDump", "Log file that needs to be dumped for Operation \"CompareIndexToLog\" ")
              .withRequiredArg().describedAs("log_file_to_dump").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outFileOpt =
          parser.accepts("outFile", "Output file to redirect the output ")
              .withRequiredArg().describedAs("outFile").ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(fileToReadOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);

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
      String fileToRead = options.valueOf(fileToReadOpt);
      String typeOfOperation = options.valueOf(typeOfOperationOpt);
      String startOffsetStr = options.valueOf(startOffsetOpt);
      String endOffsetStr = options.valueOf(endOffsetOpt);
      String logFileToDump = options.valueOf(logFileToCompareOpt);
      String outFile = options.valueOf(outFileOpt);

      long startOffset = -1;
      long endOffset = -1;
      if (startOffsetStr != null) {
        startOffset = Long.parseLong(startOffsetStr);
      }
      if (endOffsetStr != null) {
        endOffset = Long.parseLong(endOffsetStr);
      }

      String blobList = options.valueOf(listOfBlobs);
      boolean filter = (blobList != null) ? true : false;
      ArrayList<String> blobs = new ArrayList<String>();
      String[] blobArray = null;
      if (blobList != null) {
        blobArray = blobList.split(",");
        blobs.addAll(Arrays.asList(blobArray));
        System.out.println("Blobs to look out for :: " + blobs);
      }

      System.out.println("File to read " + fileToRead);
      System.out.println("Type of Operation " + typeOfOperation);

      File file = new File(fileToRead);
      DumpData dumpData = new DumpData(outFile, map);
      if (typeOfOperation.compareTo("DumpIndex") == 0) {
        dumpData.dumpIndex(file, null, null, blobs, null);
      } else if (typeOfOperation.compareTo("DumpLog") == 0) {
        dumpData.dumpLog(file, startOffset, endOffset, blobs, filter);
      } else if (typeOfOperation.compareTo("DumpReplicatoken") == 0) {
        dumpData.dumpReplicaToken(file);
      } else if (typeOfOperation.compareTo("CompareIndexToLog") == 0) {
        dumpData.compareIndexEntriestoLogContent(logFileToDump);
      } else {
        System.out.println("Unknown file to read option");
      }
      dumpData.shutdown();
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    }
  }

  public long dumpIndex(File indexFileToDump, String replica, ArrayList<String> replicaList, ArrayList<String> blobList,
      ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap) {
    long numberOfKeysProcessed = 0;
    try {
      DataInputStream stream = new DataInputStream(new FileInputStream(indexFileToDump));
      logOutput("Dumping index " + indexFileToDump.getName() + " for " + replica);
      short version = stream.readShort();
      logOutput("version " + version);
      if (version == 0) {
        int keysize = stream.readInt();
        int valueSize = stream.readInt();
        long fileEndPointer = stream.readLong();
        logOutput("key size " + keysize);
        logOutput("value size " + valueSize);
        logOutput("file end pointer " + fileEndPointer);
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
        while (stream.available() > Crc_Size) {
          StoreKey key = storeKeyFactory.getStoreKey(stream);
          byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
          stream.read(value);
          IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
          String msg =
              "key " + key + " keySize(in bytes) " + key.sizeInBytes() + " value - offset " + blobValue.getOffset()
                  + " size " + blobValue.getSize() + " Original Message Offset " + blobValue.getOriginalMessageOffset()
                  +
                  " Flag " + blobValue.isFlagSet(IndexValue.Flags.Delete_Index);
          boolean isDeleted = blobValue.isFlagSet(IndexValue.Flags.Delete_Index);
          numberOfKeysProcessed++;
          if (blobIdToStatusMap == null) {
            if (blobList == null || blobList.size() == 0 || blobList.contains(key.toString())) {
              logOutput(msg);
            }
          } else {
            if (blobIdToStatusMap.containsKey(key.toString())) {
              BlobStatus mapValue = blobIdToStatusMap.get(key.toString());
              if (isDeleted) {
                if (mapValue.getAvailable().contains(key.toString())) {
                  mapValue.getAvailable().remove(key.toString());
                }
                mapValue.addDeletedOrExpired(replica);
              } else {
                mapValue.addAvailable(replica);
                if(mapValue.getDeletedOrExpired().contains(replica)){
                  logOutput("Put Record found after delete record for " + replica);
                }
              }
            } else {
              BlobStatus mapValue = new BlobStatus(replica, isDeleted, replicaList);
              blobIdToStatusMap.put(key.toString(), mapValue);
            }
          }
          if (keysize != key.sizeInBytes()) {
            logOutput("KeySize mismatch for key " + key);
          }
        }
        logOutput("crc " + stream.readLong());
        logOutput("Total number of keys processed " + numberOfKeysProcessed);
      }
    } catch (IOException ioException) {
      logOutput("IOException thrown " + ioException);
    } catch (Exception exception) {
      logOutput("Exception thrown " + exception);
    }
    return numberOfKeysProcessed;
  }

  public void dumpLog(File file, long startOffset, long endOffset, ArrayList<String> blobs, boolean filter)
      throws IOException {
    System.out.println("Dumping log");
    long currentOffset = 0;
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    long fileSize = file.length();
    boolean lastBlobFailed = false;
    if (startOffset != -1) {
      currentOffset = startOffset;
      randomAccessFile.seek(currentOffset);
    }
    if (endOffset == -1) {
      endOffset = fileSize;
    }
    System.out.println("Starting dumping from offset " + currentOffset);
    while (currentOffset < endOffset) {
      long tempCurrentOffset = currentOffset;
      String messageheader = null;
      String blobId = null;
      String blobProperty = null;
      String usermetadata = null;
      String blobOutput = null;
      String deleteMsg = null;
      try {
        short version = randomAccessFile.readShort();
        if (version == 1) {
          ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
          buffer.putShort(version);
          randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
          buffer.flip();
          MessageFormatRecord.MessageHeader_Format_V1 header = new MessageFormatRecord.MessageHeader_Format_V1(buffer);
          messageheader = " Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() +
              " currentOffset " + currentOffset +
              " blobPropertiesRelativeOffset " + header.getBlobPropertiesRecordRelativeOffset() +
              " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() +
              " dataRelativeOffset " + header.getBlobRecordRelativeOffset() +
              " crc " + header.getCrc();
          // read blob id
          InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());
          BlobId id = new BlobId(new DataInputStream(streamlog), map);
          blobId = "Id - " + id.getID();
          boolean isDeleted = false;
          if (header.getBlobPropertiesRecordRelativeOffset()
              != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
            BlobProperties props = MessageFormatRecord.deserializeBlobProperties(streamlog);
            blobProperty = " Blob properties - blobSize  " + props.getBlobSize() +
                " serviceId " + props.getServiceId();
            ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(streamlog);
            usermetadata = " Metadata - size " + metadata.capacity();
            BlobOutput output = MessageFormatRecord.deserializeBlob(streamlog);
            blobOutput = "Blob - size " + output.getSize();
          } else {
            boolean deleteFlag = MessageFormatRecord.deserializeDeleteRecord(streamlog);
            isDeleted = true;
            deleteMsg = "delete change " + deleteFlag;
          }
          lastBlobFailed = false;
          if (!isDeleted) {
            if (filter) {
              if (blobs.contains(id.getID())) {
                System.out.println(
                    messageheader + "\n " + blobId + "\n" + blobProperty + "\n" + usermetadata + "\n" + blobOutput);
              }
            } else {
              System.out.println(
                  messageheader + "\n " + blobId + "\n" + blobProperty + "\n" + usermetadata + "\n" + blobOutput);
            }
          } else {
            if (filter) {
              if (blobs.contains(id.getID())) {
                System.out.println(messageheader + "\n " + blobId + "\n" + deleteMsg);
              }
            } else {
              System.out.println(messageheader + "\n " + blobId + "\n" + deleteMsg);
            }
          }
          currentOffset += (header.getMessageSize() + buffer.capacity() + id.sizeInBytes());
        } else {
          if (!lastBlobFailed) {
            System.out
                .println("Header Version not supported. Thrown at reading a msg starting at " + tempCurrentOffset);
            lastBlobFailed = true;
          }
          randomAccessFile.seek(++tempCurrentOffset);
          currentOffset = tempCurrentOffset;
        }
      } catch (IllegalArgumentException e) {
        System.out.println("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", " +
            "while reading blob starting at offset " + tempCurrentOffset + " with " + messageheader + blobId
            + blobProperty + usermetadata + blobOutput + " exception: " + e);
        randomAccessFile.seek(++tempCurrentOffset);
        currentOffset = tempCurrentOffset;
      } catch (MessageFormatException e) {
        System.out.println("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position() +
            " while reading blob starting at offset " + tempCurrentOffset + " with " + messageheader + blobId
            + blobProperty + usermetadata + blobOutput + " exception: " + e);
        randomAccessFile.seek(++tempCurrentOffset);
        currentOffset = tempCurrentOffset;
      } catch (EOFException e) {
        e.printStackTrace();
        System.out.println("EOFException thrown at " + randomAccessFile.getChannel().position());
        throw (e);
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println(
            "Unknown exception thrown " + e.getMessage() + "\nTrying from next offset " + (tempCurrentOffset + 1));
        randomAccessFile.seek(++tempCurrentOffset);
        currentOffset = tempCurrentOffset;
      }
    }
  }

  public void dumpReplicaToken(File replicaTokenFile)
      throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException,
             ClassNotFoundException, InvocationTargetException {
    System.out.println("Dumping replica token");
    DataInputStream stream = new DataInputStream(new FileInputStream(replicaTokenFile));
    short version = stream.readShort();
    switch (version) {
      case 0:
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
        FindTokenFactory findTokenFactory =
            Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);
        while (stream.available() > Crc_Size) {
          // read partition id
          PartitionId partitionId = map.getPartitionIdFromStream(stream);
          // read remote node host name
          String hostname = Utils.readIntString(stream);
          // read remote replica path
          String replicaPath = Utils.readIntString(stream);
          // read remote port
          int port = stream.readInt();
          // read total bytes read from local store
          long totalBytesReadFromLocalStore = stream.readLong();
          // read replica token
          FindToken token = findTokenFactory.getFindToken(stream);
          System.out.println(
              "partitionId " + partitionId + " hostname " + hostname + " replicaPath " + replicaPath + " port " + port
                  + " totalBytesReadFromLocalStore " + totalBytesReadFromLocalStore + " token " + token);
        }
        System.out.println("crc " + stream.readLong());
    }
  }


  public void compareIndexEntriestoLogContent(String logFileToDump)
      throws Exception {
    if (logFileToDump == null) {
      System.out.println("logFileToDump needs to be set for compareIndexToLog");
      System.exit(0);
    }
    DataInputStream stream = new DataInputStream(new FileInputStream(logFileToDump));
    RandomAccessFile randomAccessFile = new RandomAccessFile(new File(logFileToDump), "r");
    System.out.println("Comparing Index entries to Log ");
    short version = stream.readShort();
    System.out.println("version " + version);
    if (version == 0) {
      int keysize = stream.readInt();
      int valueSize = stream.readInt();
      long fileEndPointer = stream.readLong();
      System.out.println("key size " + keysize);
      System.out.println("value size " + valueSize);
      System.out.println("file end pointer " + fileEndPointer);
      int Crc_Size = 8;
      StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
      while (stream.available() > Crc_Size) {
        StoreKey key = storeKeyFactory.getStoreKey(stream);
        byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
        stream.read(value);
        IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
        String msg = "key :" + key + ": value - offset " + blobValue.getOffset() + " size " +
            blobValue.getSize() + " Original Message Offset " + blobValue.getOriginalMessageOffset() +
            " Flag " + blobValue.getFlags() + "\n";
        boolean success = readFromLog(randomAccessFile, blobValue.getOffset(), map, key.getID());
        if (!success) {
          System.out.println("Failed for Index Entry " + msg);
        }
      }
      System.out.println("crc " + stream.readLong());
    }
  }

  public boolean readFromLog(RandomAccessFile randomAccessFile, long offset, ClusterMap map, String blobId)
      throws Exception {

    String messageheader = null;
    String parsedBlobId = null;
    String blobProperty = null;
    String usermetadata = null;
    String blobOutput = null;
    String deleteMsg = null;
    try {
      randomAccessFile.seek(offset);
      short version = randomAccessFile.readShort();
      if (version == 1) {
        ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
        buffer.putShort(version);
        randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
        buffer.flip();
        MessageFormatRecord.MessageHeader_Format_V1 header = new MessageFormatRecord.MessageHeader_Format_V1(buffer);
        messageheader = " Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() +
            " currentOffset " + offset +
            " blobPropertiesRelativeOffset " + header.getBlobPropertiesRecordRelativeOffset() +
            " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() +
            " dataRelativeOffset " + header.getBlobRecordRelativeOffset() +
            " crc " + header.getCrc();
        // read blob id
        InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());
        BlobId id = new BlobId(new DataInputStream(streamlog), map);
        if (id.getID().compareTo(blobId) != 0) {
          System.out.println(
              "BlobId did not match the index value. BlodId from index " + blobId + ", blobid in log " + id.getID());
        }
        parsedBlobId = "Id - " + id.getID();
        boolean isDeleted = false;
        if (header.getBlobPropertiesRecordRelativeOffset()
            != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
          BlobProperties props = MessageFormatRecord.deserializeBlobProperties(streamlog);
          blobProperty = " Blob properties - blobSize  " + props.getBlobSize() +
              " serviceId " + props.getServiceId();
          ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(streamlog);
          usermetadata = " Metadata - size " + metadata.capacity();
          BlobOutput output = MessageFormatRecord.deserializeBlob(streamlog);
          blobOutput = "Blob - size " + output.getSize();
        } else {
          boolean deleteFlag = MessageFormatRecord.deserializeDeleteRecord(streamlog);
          isDeleted = true;
          deleteMsg = "delete change " + deleteFlag;
        }
        if (!isDeleted) {
          System.out.println(
              messageheader + "\n " + parsedBlobId + "\n" + blobProperty + "\n" + usermetadata + "\n" + blobOutput);
        } else {
          System.out.println(messageheader + "\n " + parsedBlobId + "\n" + deleteMsg);
        }
        return true;
      } else {
        System.out.println("Failed to parse log for blob " + blobId
            + " : Header Version not supported. Thrown at reading a msg starting at " + offset);
      }
    } catch (IllegalArgumentException e) {
      System.out.println("Illegal arg exception thrown at  " + randomAccessFile.getChannel().position() + ", " +
          "while reading blob starting at offset " + offset + " with " + messageheader + parsedBlobId + blobProperty
          + usermetadata + blobOutput + " exception: " + e);
    } catch (MessageFormatException e) {
      System.out.println("MessageFormat exception thrown at  " + randomAccessFile.getChannel().position() +
          " while reading blob starting at offset " + offset + " with " + messageheader + parsedBlobId + blobProperty
          + usermetadata + blobOutput + " exception: " + e);
    } catch (EOFException e) {
      e.printStackTrace();
      System.out.println("EOFException thrown at " + randomAccessFile.getChannel().position());
      throw (e);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Unknown exception thrown " + e.getMessage());
    }
    return false;
  }

  public synchronized  void logOutput(String msg) {
    try {
      if (outFile == null) {
        System.out.println(msg);
      } else {
          fileWriter.write(msg + "\n");
      }
    } catch (IOException e) {
      System.out.println("IOException while trying to write to File");
    }
  }

  public void shutdown() {
    try {
      if (outFile != null) {
        fileWriter.flush();
        fileWriter.close();
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to close File " + outFile);
    }
  }
}
