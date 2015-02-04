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
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> fileToReadOpt =
          parser.accepts("fileToRead", "The file that needs to be dumped").withRequiredArg().describedAs("file_to_read")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfFileOpt =
          parser.accepts("typeOfFile", "The type of file to read - log or index or replicatoken compareIndexToLog")
              .withRequiredArg().describedAs("The type of file").ofType(String.class).defaultsTo("log");

      ArgumentAcceptingOptionSpec<String> listOfBlobs =
          parser.accepts("listOfBlobs", "List Of Blobs to look for during log/index dump").withRequiredArg()
              .describedAs("List of blobs, comma separated").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> startOffsetOpt =
          parser.accepts("startOffset", "Log Offset to start dumping from").withRequiredArg().describedAs("startOffset")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> endOffsetOpt =
          parser.accepts("endOffset", "Log Offset to end dumping").withRequiredArg().describedAs("endOffset")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> logFileToDumpOpt =
          parser.accepts("logFileToDump", "Log file that needs to be dumped for Index comparison").withRequiredArg()
              .describedAs("log_file_to_dump").ofType(String.class);

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
      String typeOfFile = options.valueOf(typeOfFileOpt);
      String startOffsetStr = options.valueOf(startOffsetOpt);
      String endOffsetStr = options.valueOf(endOffsetOpt);
      String logFileToDump = options.valueOf(logFileToDumpOpt);

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
      List<String> blobs = new ArrayList<String>();
      String[] blobArray = null;
      if (blobList != null) {
        blobArray = blobList.split(",");
        blobs.addAll(Arrays.asList(blobArray));
        System.out.println("Blobs to look out for :: " + blobs);
      }

      System.out.println("File to read " + fileToRead);
      System.out.println("Type of file " + typeOfFile);

      File file = new File(fileToRead);
      DataInputStream stream = new DataInputStream(new FileInputStream(file));
      if (typeOfFile.compareTo("index") == 0) {
        System.out.println("Dumping index");
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
            String msg = "key " + key + " value - offset " + blobValue.getOffset() + " size " +
                blobValue.getSize() + " Original Message Offset " + blobValue.getOriginalMessageOffset() +
                " Flag " + blobValue.getFlags();
            if ((blobList == null) || blobs.contains(key.toString())) {
              System.out.println(msg);
            }
          }
          System.out.println("crc " + stream.readLong());
        }
      } else if (typeOfFile.compareTo("log") == 0) {
        System.out.println("Dumping log");
        long currentOffset = 0;
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        long fileSize = file.length();
        boolean lastBlobFailed = false;
        long blobsProcessed = 0;
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
              buffer.clear();
              MessageFormatRecord.MessageHeader_Format_V1 header =
                  new MessageFormatRecord.MessageHeader_Format_V1(buffer);
              messageheader = " Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() +
                  " currentOffset " + currentOffset +
                  " blobPropertiesRelativeOffset " + header.getBlobPropertiesRecordRelativeOffset() +
                  " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() +
                  " dataRelativeOffset " + header.getBlobRecordRelativeOffset() +
                  " crc " + header.getCrc();
              // read blob id
              InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());
              BlobId id = new BlobId(new DataInputStream(streamlog), map);
              blobId = "Id - " + id.toString();
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
                  if (blobs.contains(id.toString())) {
                    System.out.println(
                        messageheader + "\n " + blobId + "\n" + blobProperty + "\n" + usermetadata + "\n" + blobOutput);
                  }
                } else {
                  System.out.println(
                      messageheader + "\n " + blobId + "\n" + blobProperty + "\n" + usermetadata + "\n" + blobOutput);
                }
              } else {
                if (filter) {
                  if (blobs.contains(id.toString())) {
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
      } else if (typeOfFile.compareTo("replicatoken") == 0) {
        System.out.println("Dumping replica token");
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
                  "partitionId " + partitionId + " hostname " + hostname + " replicaPath " + replicaPath + " port "
                      + port + " totalBytesReadFromLocalStore " + totalBytesReadFromLocalStore + " token " + token);
            }
            System.out.println("crc " + stream.readLong());
        }
      } else if (typeOfFile.compareTo("compareIndexToLog") == 0) {
        DumpData dumpData = new DumpData();
        if (logFileToDump == null) {
          System.out.println("logFileToDump needs to be set for compareIndexToLog");
          System.exit(0);
        }
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
            boolean success = dumpData.readFromLog(randomAccessFile, blobValue.getOffset(), map, key.toString());
            if (!success) {
              System.out.println("Failed for Index Entry " + msg);
            }
          }
          System.out.println("crc " + stream.readLong());
        }
      } else {
        System.out.println("Unknown file to read option");
      }
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
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
        buffer.clear();
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
        if (id.toString().compareTo(blobId) != 0) {
          System.out.println(
              "BlobId dint match the index value. BlodId from index " + blobId + ", blobid in log " + id.toString());
        }
        parsedBlobId = "Id - " + id.toString();
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
}
