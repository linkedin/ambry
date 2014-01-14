package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.shared.BlobId;
import com.github.ambry.store.BlobIndexValue;
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
              parser.accepts("fileToRead", "The file that needs to be dumped")
                    .withRequiredArg()
                    .describedAs("file_to_read")
                    .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
              parser.accepts("hardwareLayout", "The path of the hardware layout file")
                    .withRequiredArg()
                    .describedAs("hardware_layout")
                    .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
              parser.accepts("partitionLayout", "The path of the partition layout file")
                    .withRequiredArg()
                    .describedAs("partition_layout")
                    .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> typeOfFileOpt =
              parser.accepts("typeOfFile", "The type of file to read - log or index")
                    .withRequiredArg()
                    .describedAs("The type of file")
                    .ofType(String.class)
                    .defaultsTo("log");

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(fileToReadOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);

      for(OptionSpec opt : listOpt) {
        if(!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath);
      String fileToRead = options.valueOf(fileToReadOpt);
      String typeOfFile = options.valueOf(typeOfFileOpt);

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
          System.out.println("key size " + keysize);
          System.out.println("value size " + valueSize);
          long totalDataToRead = file.length() - 26;
          long numberOfEntries = totalDataToRead / (keysize + valueSize);
          System.out.println("Number of entries " + numberOfEntries);
          long numberOfEntriesRead = 0;
          while (numberOfEntriesRead < numberOfEntries) {
            short idsize = stream.readShort();
            byte[] idRead = new byte[idsize];
            stream.read(idRead);
            String id = new String(idRead);
            System.out.print(id + " ");
            byte [] valueRead = new byte[BlobIndexValue.Index_Value_Size_In_Bytes];
            stream.read(valueRead);
            BlobIndexValue value = new BlobIndexValue(ByteBuffer.wrap(valueRead));
            System.out.println("offset " + value.getOffset() + " size " + value.getSize());
            numberOfEntriesRead++;
          }
          System.out.println("log end offset " + stream.readLong());
        }
      }
      else if (typeOfFile.compareTo("log") == 0)
      {
        System.out.println("Dumping log");
        while (true) {
          short version = stream.readShort();
          if (version == 1) {
            ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
            buffer.putShort(version);
            stream.read(buffer.array(), 2, buffer.capacity() - 2);
            buffer.clear();
            MessageFormatRecord.MessageHeader_Format_V1 header = new MessageFormatRecord.MessageHeader_Format_V1(buffer);
            System.out.println(" Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() +
                    " blobPropertyRelativeOffset " + header.getBlobPropertyRecordRelativeOffset() +
                    " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() +
                    " dataRelativeOffset " + header.getBlobRecordRelativeOffset() +
                    " crc " + header.getCrc());
            // read blob id
            BlobId id = new BlobId(stream, map);
            System.out.println("Id - " + id.toString());
            BlobProperties props = MessageFormatRecord.deserializeBlobProperties(stream);
            System.out.println(" Blob properties - blobSize  " + props.getBlobSize() +
                    " serviceId " + props.getServiceId());
            ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(stream);
            System.out.println(" Metadata - size " + metadata.capacity());
            BlobOutput output = MessageFormatRecord.deserializeBlob(stream);
            System.out.println("Blob - size " + output.getSize());
          }
        }
      }
      else
        System.out.println("Unknown file to read option");
    }
    catch (Exception e) {
      System.out.println("Closed with error " + e);
    }
  }
}
