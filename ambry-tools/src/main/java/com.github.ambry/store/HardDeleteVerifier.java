/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.SubRecord;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.List;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


public class HardDeleteVerifier {
  private final ClusterMap map;
  private final String outFile;
  private final String dataDir;
  private final String oldDataDir;
  private HashMap<BlobId, IndexValue> rangeMap;
  private HashMap<BlobId, IndexValue> offRangeMap;
  private static final short HARD_DELETE_TOKEN_V0 = 0;

  /* The statistics we need to compute.*/

  // This says whether there were any entries in the log that could not be deserialized or had invalid version etc.
  private boolean invalidEntriesInlog = false;

  // Number of blobs that have a mismatch with their corresponding entry in the original log - the
  // number of undeleted blobs that are not exactly the same as the blob read from the same offset in the original
  // replica. This should be 0.
  private long mismatchWithOldErrorCount = 0;

  // Same as above, but provides the number of blobs that are zeroed out in the new log that were deleted after the
  // last eligible segment. Ideally this should be 0.
  // Basically, there could have been deletes between the startToken (which determines the last eligible segment)
  // and the endToken that the rangeMap (which has entries till the last eligible segment) does not reflect. The
  // hard delete thread could have processed these deletes, so a mismatch that we see could be due to that.
  // Therefore, those kind of mismatches will be counted toward this count and not towards
  // mismatchWithOldErrorCount.
  private long mismatchAccountedInNewerSegments = 0;

  // Number of deleted blobs before the end point represented by the cleanup token, which do not have the blob
  // properties and the content zeroed out. This should be 0.
  private long notHardDeletedErrorCount = 0;

  // (Approximate) Number of times multiple put records for the same blob was encountered.
  private long duplicatePuts = 0;

  // Number of put records that are still untouched.
  private long unDeletedPuts = 0;

  // Number of records that are deleted before the end point represented by the cleanup token.
  private long hardDeletedPuts = 0;

  // Number of delete records encountered.
  private long deletes = 0;

  // Number of non deleted blobs that are corrupt.
  private long corruptNonDeleted = 0;

  // Number of deleted blobs that are corrupt.
  private long corruptDeleted = 0;

  public HardDeleteVerifier(ClusterMap map, String dataDir, String oldDataDir, String outFile) {
    this.map = map;
    this.dataDir = dataDir;
    this.oldDataDir = oldDataDir;
    this.outFile = outFile;
  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
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

      ArgumentAcceptingOptionSpec<String> dataDirOpt = parser.accepts("dataDir",
          "The data directory of the partition/replica that needs to be verified for hard deletes.")
          .withRequiredArg()
          .describedAs("data_dir")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> oldDataDirOpt = parser.accepts("oldDataDir",
          "[Optional] The data directory of the partition/replica before hard deletes are run for comparison")
          .withOptionalArg()
          .describedAs("old_data_dir")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outFileOpt = parser.accepts("outFile", "Output file to redirect to ")
          .withRequiredArg()
          .describedAs("outFile")
          .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec> requiredOpts = new ArrayList<>();
      requiredOpts.add(hardwareLayoutOpt);
      requiredOpts.add(partitionLayoutOpt);
      requiredOpts.add(dataDirOpt);
      requiredOpts.add(outFileOpt);

      ToolUtils.ensureOrExit(requiredOpts, options, parser);

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(new Properties()));
      ClusterMap map =
          ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      String dataDir = options.valueOf(dataDirOpt);
      String oldDataDir = options.has(oldDataDirOpt) ? options.valueOf(oldDataDirOpt) : null;
      String outFile = options.valueOf(outFileOpt);

      HardDeleteVerifier hardDeleteVerifier = new HardDeleteVerifier(map, dataDir, oldDataDir, outFile);
      hardDeleteVerifier.verifyHardDeletes();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class ContinueException extends Exception {
    ContinueException(String s) {
      super(s);
    }
  }

  private long getOffsetFromCleanupToken(File cleanupTokenFile) throws Exception {
    long parsedTokenValue = -1;
    if (cleanupTokenFile.exists()) {
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(cleanupTokenFile));
      DataInputStream stream = new DataInputStream(crcStream);
      try {
        // The format of the cleanup token is documented in PersistentIndex.persistCleanupToken()
        short version = stream.readShort();
        if (version != HARD_DELETE_TOKEN_V0) {
          throw new IllegalStateException("Unknown version encountered while parsing cleanup token");
        }
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
        FindTokenFactory factory = Utils.getObj("com.github.ambry.store.StoreFindTokenFactory", storeKeyFactory);

        FindToken startToken = factory.getFindToken(stream);
        //read past the end token.
        factory.getFindToken(stream);

        ByteBuffer bytebufferToken = ByteBuffer.wrap(startToken.toBytes());
        short tokenVersion = bytebufferToken.getShort();
        if (tokenVersion != 0) {
          throw new IllegalArgumentException("token version: " + tokenVersion + " is unknown");
        }
        int sessionIdsize = bytebufferToken.getInt();
        bytebufferToken.position(bytebufferToken.position() + sessionIdsize);
        parsedTokenValue = bytebufferToken.getLong();
        if (parsedTokenValue == -1) {
          /* Index based token, get index start offset */
          parsedTokenValue = bytebufferToken.getLong();
        }

        /* Just read the remaining fields and verify that the crc matches. We don't really need the fields for this
           test */
        int num = stream.readInt();
        List<StoreKey> storeKeyList = new ArrayList<StoreKey>(num);
        for (int i = 0; i < num; i++) {
          // Read BlobReadOptions
          short blobReadOptionsVersion = stream.readShort();
          if (blobReadOptionsVersion != 0) {
            throw new IllegalStateException("Unknown blobReadOptionsVersion: " + blobReadOptionsVersion);
          }
          long offset = stream.readLong();
          long sz = stream.readLong();
          long ttl = stream.readLong();
          StoreKey key = storeKeyFactory.getStoreKey(stream);
          storeKeyList.add(key);
        }
        for (int i = 0; i < num; i++) {
          int length = stream.readInt();
          short headerVersion = stream.readShort();
          short userMetadataVersion = stream.readShort();
          int userMetadataSize = stream.readInt();
          short blobRecordVersion = stream.readShort();
          long blobStreamSize = stream.readLong();
          StoreKey key = storeKeyFactory.getStoreKey(stream);
          if (!storeKeyList.get(i).equals(key)) {
            throw new IllegalStateException("Parsed key mismatch");
          }
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

  /**
   * @param offsetUpto - the entries upto this offset are put into rangeMap, rest are put into offRangeMap
   * @return the last offset in the rangeMap.
   * @throws Exception
   */
  private long readAndPopulateIndex(long offsetUpto) throws Exception {
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

    /* The idea is to populate everything till the offsetUpto in the rangeMap, rest in offRangeMap */

    HashMap<BlobId, IndexValue> blobMap = rangeMap;
    int numberOfKeysProcessed = 0;
    for (File indexFile : indexFiles) {
      DataInputStream stream = null;
      try {
        Offset segmentStartOffset = IndexSegment.getIndexSegmentStartOffset(indexFile.getName());
        /* Read each index file as long as it is within the endToken and populate a map with the status of the blob.*/
        stream = new DataInputStream(new FileInputStream(indexFile));
        short version = stream.readShort();
        if (version != 0) {
          throw new IllegalStateException("Unknown index file version: " + version);
        }
        int keysize = stream.readInt();
        int valueSize = stream.readInt();
        long segmentEndOffset = stream.readLong();
        if (segmentStartOffset.getOffset() > offsetUpto) {
          if (!blobMap.equals(offRangeMap)) {
            System.out.println(
                "Reached the last segment with segment start offset " + segmentStartOffset + " greater than offsetUpto "
                    + offsetUpto);
            //switch to offRangeMap for subsequent entries.
            blobMap = offRangeMap;
          }
        } else {
          lastEligibleSegmentEndOffset = segmentEndOffset;
        }
        int Crc_Size = 8;
        StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", map);
        while (stream.available() > Crc_Size) {
          BlobId key = (BlobId) storeKeyFactory.getStoreKey(stream);
          int valueByteSize;
          switch (version) {
            case PersistentIndex.VERSION_0:
              valueByteSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0;
              break;
            case PersistentIndex.VERSION_1:
            case PersistentIndex.VERSION_2:
              valueByteSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2;
              break;
            case PersistentIndex.VERSION_3:
              valueByteSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3;
              break;
            default:
              throw new IllegalArgumentException("Unknown PersistentIndex Version.");
          }
          byte[] value = new byte[valueByteSize];
          stream.read(value);
          IndexValue blobValue = new IndexValue(segmentStartOffset.getName(), ByteBuffer.wrap(value), version);
          boolean deleted = blobValue.isFlagSet(IndexValue.Flags.Delete_Index);
          numberOfKeysProcessed++;
          IndexValue oldValue = blobMap.get(key);
          if (oldValue != null) {
            // If there was an old entry for the same key, then ensure that the old value was not a delete
            // and the new value *is* a delete.
            if (oldValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              System.err.println("Old value was a delete and is getting replaced!");
            }
            if (!blobValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              System.err.println("New value is not a delete!");
            }
          }
          blobMap.put(key, blobValue);
        }
      } finally {
        if (stream != null) {
          stream.close();
        }
      }
    }

    System.out.println("Total number of keys processed " + numberOfKeysProcessed);
    return lastEligibleSegmentEndOffset;
  }

  private boolean verifyZeroed(byte[] arr) {
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method ensures that all the blobs that should have been hard deleted are indeed hard deleted and the rest
   * are untouched. Optionally, if the state of the dataDir prior to hard deletes being enabled is available, then
   * compares the non-hard deleted records between the two and ensure they are exactly the same.
   *
   * Here's the algorithm:
   *
   * 1. Reads cleanupToken and gets the conservative offset till which hard deletes surely are complete.
   * 2. Reads the index files and stores everything upto the conservative offset above in a "rangeMap" and the later
   *    entries in an "offRangeMap".
   * 3. Goes through the log file upto the conservative offset, and for each entry checks whether deserialization
   *    happens successfully, whether it is zeroed out if it is deleted, whether the content is the same as in the
   *    older data file (if there is one) if it undeleted, whether duplicate puts were encountered etc.
   *
   * @throws IOException
   */

  public void verifyHardDeletes() throws Exception {
    if (oldDataDir != null) {
      verify(dataDir, oldDataDir);
    } else {
      verify(dataDir);
    }
  }

  private void verify(String dataDir) throws Exception {
    final String Cleanup_Token_Filename = "cleanuptoken";

    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(new File(outFile));
      long offsetInCleanupToken = getOffsetFromCleanupToken(new File(dataDir, Cleanup_Token_Filename));
      rangeMap = new HashMap<BlobId, IndexValue>();
      offRangeMap = new HashMap<BlobId, IndexValue>();
      long lastEligibleSegmentEndOffset = readAndPopulateIndex(offsetInCleanupToken);

      // 2. Scan the log and check against blobMap
      File logFile = new File(dataDir, "log_current");
      RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
      InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());

      long currentOffset = 0;
      System.out.println("Starting scan from offset " + currentOffset + " to " + offsetInCleanupToken);

      long lastOffsetToLookFor = lastEligibleSegmentEndOffset;
      boolean seeking = false;
      while (currentOffset < lastOffsetToLookFor) {
        try {
          short version = randomAccessFile.readShort();
          if (version == 1) {
            seeking = false;
            ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
            buffer.putShort(version);
            randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
            buffer.rewind();
            MessageFormatRecord.MessageHeader_Format_V1 header =
                new MessageFormatRecord.MessageHeader_Format_V1(buffer);

            // read blob id
            BlobId id;
            id = new BlobId(new DataInputStream(streamlog), map);

            IndexValue indexValue = rangeMap.get(id);
            boolean isDeleted = false;
            if (indexValue == null) {
              throw new IllegalStateException("Key in log not found in index " + id);
            } else if (indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
              isDeleted = true;
            }

            if (header.getBlobPropertiesRecordRelativeOffset()
                != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
              BlobProperties props;
              ByteBuffer metadata;
              BlobData output;
              try {
                props = MessageFormatRecord.deserializeBlobProperties(streamlog);
                metadata = MessageFormatRecord.deserializeUserMetadata(streamlog);
                output = MessageFormatRecord.deserializeBlob(streamlog);
              } catch (MessageFormatException e) {
                if (!isDeleted) {
                  corruptNonDeleted++;
                } else {
                  corruptDeleted++;
                }
                throw e;
              }

              if (isDeleted) {
                ByteBuf byteBuf = output.content();
                try {
                  if (!verifyZeroed(metadata.array()) || !verifyZeroed(
                      Utils.readBytesFromByteBuf(byteBuf, new byte[(int) output.getSize()], 0,
                          (int) output.getSize()))) {
                    /* If the offset in the index is different from that in the log, hard delete wouldn't have been
                       possible and we just saw a duplicate put for the same key, otherwise we missed a hard delete. */
                    if (currentOffset == indexValue.getOriginalMessageOffset()) {
                      notHardDeletedErrorCount++;
                    } else {
                      // the assumption here is that this put has been lost as far as the index is concerned due to
                      // a duplicate put. Of course, these shouldn't happen anymore, we are accounting for past
                      // bugs.
                      duplicatePuts++;
                    }
                  } else {
                    hardDeletedPuts++;
                  }
                } finally {
                  byteBuf.release();
                }
              } else {
                unDeletedPuts++;
              }
            } else if (MessageFormatRecord.deserializeUpdateRecord(streamlog).getType().equals(SubRecord.Type.DELETE)) {
              deletes++;
            }
            currentOffset += (header.getMessageSize() + buffer.capacity() + id.sizeInBytes());
          } else {
            throw new IllegalStateException("Unknown version for entry");
          }
        } catch (MessageFormatException e) {
          if (!seeking) {
            invalidEntriesInlog = true;
            e.printStackTrace();
            seeking = true;
          }
          randomAccessFile.seek(++currentOffset);
        } catch (IOException e) {
          if (!seeking) {
            invalidEntriesInlog = true;
            e.printStackTrace();
            seeking = true;
          }
          randomAccessFile.seek(++currentOffset);
        } catch (IllegalArgumentException e) {
          if (!seeking) {
            invalidEntriesInlog = true;
            e.printStackTrace();
            seeking = true;
          }
          randomAccessFile.seek(++currentOffset);
        } catch (IllegalStateException e) {
          if (!seeking) {
            invalidEntriesInlog = true;
            e.printStackTrace();
            seeking = true;
          }
          randomAccessFile.seek(++currentOffset);
        } catch (Exception e) {
          e.printStackTrace(System.err);
          invalidEntriesInlog = true;
          randomAccessFile.seek(++currentOffset);
          break;
        }
      }
      String msg = ("\n============");
      msg += "\ninvalidEntriesInlog? " + (invalidEntriesInlog ? "Yes" : "No");
      msg += "\nnotHardDeletedErrorCount: " + notHardDeletedErrorCount;
      msg += "\ncorruptNonDeletedCount:" + corruptNonDeleted;
      msg += "\n========";
      msg += "\ncorruptDeleted:" + corruptDeleted;
      msg += "\nduplicatePuts: " + duplicatePuts;
      msg += "\nundeleted Put Records: " + unDeletedPuts;
      msg += "\nhard deleted Put Records: " + hardDeletedPuts;
      msg += "\nDelete Records: " + deletes;
      msg += "\n============";
      fileWriter.write(msg);
      System.out.println(msg);
    } finally {
      if (fileWriter != null) {
        fileWriter.flush();
        fileWriter.close();
      }
    }
  }

  private void verify(String dataDir, String oldDataDir) throws Exception {
    final String Cleanup_Token_Filename = "cleanuptoken";

    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(new File(outFile));
      long offsetInCleanupToken = getOffsetFromCleanupToken(new File(dataDir, Cleanup_Token_Filename));
      rangeMap = new HashMap<BlobId, IndexValue>();
      offRangeMap = new HashMap<BlobId, IndexValue>();
      long lastEligibleSegmentEndOffset = readAndPopulateIndex(offsetInCleanupToken);

      // 2. Scan the log and check against blobMap
      File logFile = new File(dataDir, "log_current");
      RandomAccessFile randomAccessFile = new RandomAccessFile(logFile, "r");
      InputStream streamlog = Channels.newInputStream(randomAccessFile.getChannel());

      File oldLogFile = new File(oldDataDir, "log_current");
      RandomAccessFile oldRandomAccessFile = new RandomAccessFile(oldLogFile, "r");
      InputStream oldStreamlog = Channels.newInputStream(oldRandomAccessFile.getChannel());

      long currentOffset = 0;
      System.out.println("Starting scan from offset " + currentOffset + " to " + offsetInCleanupToken);

      long lastOffsetToLookFor = Math.min(lastEligibleSegmentEndOffset, oldRandomAccessFile.length());
      boolean seeking = false;
      while (currentOffset < lastOffsetToLookFor) {
        boolean mismatchWithOld = false;
        try {
          oldRandomAccessFile.seek(randomAccessFile.getFilePointer());
          short version = randomAccessFile.readShort();
          if (version == 1) {
            seeking = false;
            ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
            buffer.putShort(version);
            randomAccessFile.read(buffer.array(), 2, buffer.capacity() - 2);
            buffer.rewind();
            MessageFormatRecord.MessageHeader_Format_V1 header =
                new MessageFormatRecord.MessageHeader_Format_V1(buffer);


            /* Verify that the header is the same in old and new log files */
            ByteBuffer oldBuffer = ByteBuffer.allocate(buffer.capacity());
            oldRandomAccessFile.read(oldBuffer.array(), 0, oldBuffer.capacity());
            mismatchWithOld = !Arrays.equals(buffer.array(), oldBuffer.array());

            BlobId id = readBlobId(streamlog, oldStreamlog);

            boolean isDeleted = false;
            boolean proceed = true;
            IndexValue indexValue = readIndexValueFromRangeMap(id);
            if (indexValue == null) {
              proceed = false;
            } else {
              isDeleted = indexValue.isFlagSet(IndexValue.Flags.Delete_Index);
            }

            if (proceed) {
              if (header.getBlobPropertiesRecordRelativeOffset()
                  != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
                deserializeBlobProperties(streamlog, oldStreamlog, isDeleted);
                boolean asExpected = deserializeUserMetadataAndBlob(streamlog, oldStreamlog, isDeleted);
                if (!asExpected) {
                  if (isDeleted) {
                    if (indexValue.getOriginalMessageOffset() != currentOffset) {
                      duplicatePuts++;
                    } else {
                      notHardDeletedErrorCount++;
                    }
                  } else {
                    indexValue = offRangeMap.get(id);
                    if (indexValue != null && indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
                      mismatchAccountedInNewerSegments++;
                    } else {
                      mismatchWithOld = true;
                    }
                  }
                } else {
                  if (isDeleted) {
                    hardDeletedPuts++;
                  } else {
                    unDeletedPuts++;
                  }
                }
              } else if (deserializeUpdateRecord(streamlog, oldStreamlog)) {
                deletes++;
              }
            }
            currentOffset += header.getMessageSize() + buffer.capacity() + id.sizeInBytes();
          } else {
            throw new ContinueException("Version mismatch");
          }
        } catch (ContinueException e) {
          if (!seeking) {
            invalidEntriesInlog = true;
            e.printStackTrace();
            seeking = true;
          }
          randomAccessFile.seek(++currentOffset);
        } catch (Exception e) {
          e.printStackTrace(System.err);
          break;
        } finally {
          if (mismatchWithOld) {
            mismatchWithOldErrorCount++;
          }
        }
      }
      String msg = ("\n============");
      msg += "\ninvalidEntriesInlog? " + (invalidEntriesInlog ? "Yes" : "No");
      if (oldDataDir != null) {
        msg += "\nmismatchWithOldErrorCount: " + mismatchWithOldErrorCount;
      }
      msg += "\nnotHardDeletedErrorCount: " + notHardDeletedErrorCount;
      msg += "\ncorruptNonDeletedCount:" + corruptNonDeleted;
      msg += "\n========";
      msg += "\nmismatchAccountedInNewerSegments: " + mismatchAccountedInNewerSegments;
      msg += "\ncorruptDeleted:" + corruptDeleted;
      msg += "\nduplicatePuts: " + duplicatePuts;
      msg += "\nundeleted Put Records: " + unDeletedPuts;
      msg += "\nhard deleted Put Records: " + hardDeletedPuts;
      msg += "\nDelete Records: " + deletes;
      msg += "\n============";
      fileWriter.write(msg);
      System.out.println(msg);
    } finally {
      if (fileWriter != null) {
        fileWriter.flush();
        fileWriter.close();
      }
    }
  }

  /**
   * @return {@code true} if this was a delete record, {@code false} otherwise
   * @throws ContinueException if there is a deser failure in the new log
   */
  private boolean deserializeUpdateRecord(InputStream streamlog, InputStream oldStreamlog) throws ContinueException {
    boolean isDeleteRecord = false;
    boolean caughtException = false;
    boolean caughtExceptionInOld = false;
    try {
      isDeleteRecord = MessageFormatRecord.deserializeUpdateRecord(streamlog).getType().equals(SubRecord.Type.DELETE);
    } catch (Exception e) {
      caughtException = true;
    }

    try {
      MessageFormatRecord.deserializeUpdateRecord(oldStreamlog);
    } catch (Exception e) {
      caughtExceptionInOld = true;
    }

    if (caughtException && !caughtExceptionInOld) {
      throw new ContinueException("delete record could not be deserialized");
    }
    return isDeleteRecord;
  }

  boolean deserializeUserMetadataAndBlob(InputStream streamlog, InputStream oldStreamlog, boolean isDeleted)
      throws ContinueException {
    boolean caughtException = false;
    boolean caughtExceptionInOld = false;
    ByteBuffer usermetadata = null;
    ByteBuffer oldUsermetadata = null;
    BlobData blobData = null;
    BlobData oldBlobData = null;

    try {
      usermetadata = MessageFormatRecord.deserializeUserMetadata(streamlog);
      blobData = MessageFormatRecord.deserializeBlob(streamlog);
    } catch (MessageFormatException e) {
      caughtException = true;
    } catch (IOException e) {
      caughtException = true;
    }

    try {
      oldUsermetadata = MessageFormatRecord.deserializeUserMetadata(oldStreamlog);
      oldBlobData = MessageFormatRecord.deserializeBlob(oldStreamlog);
    } catch (MessageFormatException e) {
      caughtExceptionInOld = true;
    } catch (IOException e) {
      caughtExceptionInOld = true;
    }

    boolean asExpected;

    if (!caughtException) {
      if (isDeleted) {
        ByteBuf byteBuf = blobData.content();
        try {
          asExpected = verifyZeroed(usermetadata.array()) && verifyZeroed(
              Utils.readBytesFromByteBuf(byteBuf, new byte[(int) blobData.getSize()], 0, (int) blobData.getSize()));
        } catch (IOException e) {
          asExpected = false;
        } finally {
          byteBuf.release();
        }
      } else {
        ByteBuf byteBuf = blobData.content();
        ByteBuf oldByteBuf = oldBlobData.content();
        try {
          asExpected = Arrays.equals(usermetadata.array(), oldUsermetadata.array()) && Arrays.equals(
              Utils.readBytesFromByteBuf(byteBuf, new byte[(int) blobData.getSize()], 0, (int) blobData.getSize()),
              Utils.readBytesFromByteBuf(oldByteBuf, new byte[(int) oldBlobData.getSize()], 0,
                  (int) oldBlobData.getSize()));
        } catch (IOException e) {
          asExpected = false;
        } finally {
          byteBuf.release();
          oldByteBuf.release();
        }
      }
      return asExpected;
    } else if (!caughtExceptionInOld) {
      if (isDeleted) {
        corruptDeleted++;
      } else {
        corruptNonDeleted++;
      }
      throw new ContinueException("records did not deserialize");
    } else {
      throw new ContinueException("records did not deserialize in either.");
    }
  }

  boolean deserializeBlobProperties(InputStream streamlog, InputStream oldStreamlog, boolean isDeleted)
      throws ContinueException {
    boolean caughtException = false;
    boolean caughtExceptionInOld = false;
    BlobProperties props = null;
    BlobProperties oldProps = null;

    try {
      props = MessageFormatRecord.deserializeBlobProperties(streamlog);
    } catch (MessageFormatException e) {
      caughtException = true;
    } catch (IOException e) {
      caughtException = true;
    }

    try {
      oldProps = MessageFormatRecord.deserializeBlobProperties(oldStreamlog);
    } catch (MessageFormatException e) {
      caughtExceptionInOld = true;
    } catch (IOException e) {
      caughtExceptionInOld = true;
    }

    if (!caughtException) {
      if (props.toString().compareTo(oldProps.toString()) != 0) {
        System.out.println("Blob id mismatch!");
        return false;
      }
    } else if (!caughtExceptionInOld) {
      if (isDeleted) {
        corruptDeleted++;
      } else {
        corruptNonDeleted++;
      }
      throw new ContinueException("blob properties could not be deserialized.");
    } else {
      throw new ContinueException("blob properties could not be deserialized in either");
    }
    return true;
  }

  IndexValue readIndexValueFromRangeMap(BlobId id) throws ContinueException {
    IndexValue indexValue = rangeMap.get(id);
    if (indexValue == null) {
      return null;
    }
    return indexValue;
  }

  BlobId readBlobId(InputStream streamlog, InputStream oldStreamlog) throws ContinueException {
    BlobId id = null;
    BlobId idInOld = null;
    boolean caughtException = false;
    boolean caughtExceptionInOld = false;

    try {
      id = new BlobId(new DataInputStream(streamlog), map);
    } catch (IOException e) {
      caughtException = true;
    } catch (IllegalArgumentException e) {
      caughtException = true;
    }

    try {
      idInOld = new BlobId(new DataInputStream(oldStreamlog), map);
    } catch (IOException e) {
      caughtExceptionInOld = true;
    } catch (IllegalArgumentException e) {
      caughtExceptionInOld = true;
    }

    if (!caughtException) {
      if (id.compareTo(idInOld) != 0) {
        throw new ContinueException("id mismatch");
      }
    } else if (!caughtExceptionInOld) {
      throw new ContinueException("blob id could not be deserialized");
    } else {
      throw new ContinueException("blob id could not be deserialized in either.");
    }
    return id;
  }
}
