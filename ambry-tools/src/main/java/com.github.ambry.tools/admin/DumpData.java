package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.shared.BlobId;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.IndexValue;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.Utils;

import java.io.*;
import java.nio.channels.Channels;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.nio.ByteBuffer;
import java.util.ArrayList;


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
          parser.accepts("typeOfFile", "The type of file to read - log or index or replicatoken").withRequiredArg()
              .describedAs("The type of file").ofType(String.class).defaultsTo("log");

      ArgumentAcceptingOptionSpec<String> ignoreErrorsOpt =
              parser.accepts("ignoreErrors", "IgnoreErrors and proceed until end of log").withRequiredArg()
                      .describedAs("To ignore errors or not").ofType(String.class).defaultsTo("falses");

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
      boolean ignoreErrors = Boolean.parseBoolean(options.valueOf(ignoreErrorsOpt));

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
          StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.shared.BlobIdFactory", map);
          while (stream.available() > Crc_Size) {
            StoreKey key = storeKeyFactory.getStoreKey(stream);
            byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
            stream.read(value);
            IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
            System.out.println("key " + key + " value - offset " + blobValue.getOffset() + " size " +
                blobValue.getSize() + " Original Message Offset " + blobValue.getOriginalMessageOffset() +
                " Flag " + blobValue.getFlags());
          }
          System.out.println("crc " + stream.readLong());
        }
      } else if (typeOfFile.compareTo("log") == 0) {
        System.out.println("Dumping log");
        long currentOffset = 0;
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        long fileSize = file.length();
        while (currentOffset < fileSize) {
          long startOffset = randomAccessFile.getChannel().position();
          try{
          short version = randomAccessFile.readShort();
              String messageheader = null;
              String id = null;
              String blobProperty = null;
              String usermetadata = null;
              String blobOutput = null;

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
            BlobId blobId = new BlobId(new DataInputStream(streamlog), map);
            id = "Id - " + blobId.toString();
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
              System.out.println("delete change " + deleteFlag);
            }
              System.out.println(messageheader +"\n " + id + "\n" + blobProperty + "\n" +usermetadata +"\n" + blobOutput );
            currentOffset += (header.getMessageSize() + buffer.capacity() + blobId.sizeInBytes());
          }
        }
          catch(IllegalArgumentException e) {
            e.printStackTrace();
            if(ignoreErrors) {
              System.out.println("IllegalArgumentException at "+ randomAccessFile.getChannel().position()+
                    ". Hence moving the offset to next byte from current offset "+(startOffset+1)) ;
              currentOffset = startOffset + 1;
              randomAccessFile.seek(currentOffset);
            }
            else{
                System.exit(1);
            }
          }
          catch(MessageFormatException e) {
            e.printStackTrace();
            if(ignoreErrors) {
              System.out.println("MessageFormatException at " + randomAccessFile.getChannel().position() +
                      ". Hence moving the offset to next byte from current offset " + (startOffset + 1)) ;
              currentOffset = startOffset + 1;
              randomAccessFile.seek(++startOffset);
            }
            else{
                System.exit(1);
            }

          }
          catch(EOFException e){
            e.printStackTrace();
            System.out.println("Exiting due to EOFException ");
            System.exit(1);
          }
          catch(Exception e) {
            e.printStackTrace();
            if(ignoreErrors) {
              System.out.println("Exception at "+ randomAccessFile.getChannel().position()+
                      ". Hence moving the offset to next byte from current offset "+(startOffset+1));
              currentOffset = startOffset+1;
              randomAccessFile.seek(currentOffset);
            }
            else{
              System.exit(1);
            }
          }
      }
      } else if (typeOfFile.compareTo("replicatoken") == 0) {
        System.out.println("Dumping replica token");
        short version = stream.readShort();
        switch (version) {
          case 0:
            int Crc_Size = 8;
            StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.shared.BlobIdFactory", map);
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
      } else {
        System.out.println("Unknown file to read option");
      }
    } catch (Exception e) {
      System.out.println("Closed with error " + e);
    }
  }
}
